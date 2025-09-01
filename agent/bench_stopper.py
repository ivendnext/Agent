import sys
import os
import time
import datetime
import signal
import json
from pathlib import Path

import docker
import requests
from filelock import FileLock, Timeout


class Config:
    check_interval_minutes: int = 15
    inactive_threshold_hours: int = 3
    min_uptime_hours: int = 3
    access_log_path: str = "/var/log/nginx"
    stats_file: str = "/home/frappe/agent/bench-memory-stats.json"
    cadvisor_endpoint: str = "http://127.0.0.1:9338/api/v1.3/docker"


class BenchStopper:
    """Monitors Docker containers based on nginx access log modification times."""

    def __init__(self):
        self.running = False
        self.sleeping = False
        self.docker_client = None
        self.mem_stats = None
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.log(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        if self.sleeping:
            sys.exit(0)

    def _init_docker_client(self) -> docker.DockerClient:
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            self.log(f"Failed to connect to Docker: {e}")
            raise

        return self.docker_client

    def _get_container_current_memory(self, container):
        try:
            stats = container.stats(stream=False)
            memory_stats = stats.get('memory_stats', {})
            return memory_stats.get('usage')
        except Exception as e:
            self.log(f"Could not get memory stats for {container.name}: {e}")

    def _get_cadvisor_memory_stats(self, container_id: str):
        try:
            # Get container stats from cAdvisor
            response = requests.get( f"{Config.cadvisor_endpoint}/{container_id}", timeout=15)
            if response.status_code != 200:
                self.log(f"cAdvisor request failed with status {response.status_code}")
                return None, None

            data = response.json()
            container_data = next(iter(data.values())) # assuming the 1st value in dict will be the cdata
            if not container_data:
                self.log(f"Data not found for {container_id} in cAdvisor data")
                return None, None

            stats = container_data.get('stats', [])

            # Get the latest stats entry (should be the most recent)
            latest_stat = stats[-1] if stats else None
            if not latest_stat:
                self.log(f"No stats found for container {container_id}")
                return None, None

            memory_stats = latest_stat.get('memory', {})
            return memory_stats.get('usage'), memory_stats.get('max_usage')
        except Exception as e:
            self.log(f"Error fetching cAdvisor data for {container_id}: {e}")

        return None, None

    def _calculate_memory_average(self, container_name: str, current_memory: int, max_memory: int) -> int:
        # Calculate average of current and max memory
        session_average = (current_memory + max_memory) // 2

        # Get previous average if it exists
        if previous_average := self.mem_stats.get(container_name):
            # Average the session average with the previous average
            final_average = (session_average + previous_average) // 2
        else:
            # No previous data, use session average
            final_average = session_average

        return final_average

    def _update_container_memory_usage(self, container):
        """Get container memory usage statistics and update mem_stats."""
        # Get both current and max memory usage from cAdvisor
        current_memory, max_memory = self._get_cadvisor_memory_stats(container.id)

        if not current_memory:
            # fallback: Get current memory usage from container API
            current_memory = self._get_container_current_memory(container)

        if not current_memory:
            return

        # Calculate averaged memory usage
        max_memory = max_memory or current_memory
        averaged_memory = self._calculate_memory_average(container.name, current_memory, max_memory)
        self.mem_stats[container.name] = averaged_memory

    def _find_log_files_for_bench(self, bench_name: str):
        """Find log files associated with a bench."""
        log_files = []
        access_log_dir = Path(Config.access_log_path)

        if not access_log_dir.exists():
            return

        for pattern in [
            f"{bench_name}-access.log",
        ]:
            found_files = list(access_log_dir.glob(pattern))
            log_files.extend(found_files)

        return set(log_files)

    def _get_last_activity_time(self, log_files):
        """Get the most recent modification time from log files."""
        latest_mtime = None

        for log_file in log_files:
            try:
                mtime = log_file.stat().st_mtime
                file_datetime = datetime.datetime.fromtimestamp(mtime)

                if latest_mtime is None or file_datetime > latest_mtime:
                    latest_mtime = file_datetime

            except Exception as e:
                self.log(f"Error checking modification time for {log_file}: {e}")

        return latest_mtime

    def _should_stop_container(self, container):
        """Determine if a container should be stopped based on activity and uptime."""
        should_stop = False

        try:
            # Check container uptime
            start_time_str = container.attrs["State"]["StartedAt"]
            start_time = datetime.datetime.fromisoformat(
                start_time_str.rstrip('Z')[:26]
            ).replace(tzinfo=datetime.timezone.utc)

            now = datetime.datetime.now()
            uptime = now.astimezone(datetime.timezone.utc) - start_time

            # Don't stop containers that haven't been running long enough
            if uptime < datetime.timedelta(hours=Config.min_uptime_hours):
                self.log(f"Container {container.name} uptime {uptime} is below minimum threshold")
                return should_stop

            # Find associated log files
            log_files = self._find_log_files_for_bench(container.name)
            if not log_files:
                self.log(f"No log files found for bench {container.name}")
                # Fall back to uptime-only check if no logs found
                should_stop = uptime > datetime.timedelta(hours=Config.inactive_threshold_hours)
                self.log(f"No logs found for {container.name}, using uptime-only check: {should_stop}")
                return should_stop

            # Check last activity based on file modification times
            last_activity = self._get_last_activity_time(log_files)
            if last_activity is None:
                self.log(f"Could not determine last activity for {container.name}")
                return uptime > datetime.timedelta(hours=Config.inactive_threshold_hours)

            # Calculate time since last activity
            time_since_activity = now - last_activity
            should_stop = time_since_activity > datetime.timedelta(hours=Config.inactive_threshold_hours)

        except Exception as e:
            self.log(f"Error in _should_stop_container for container {container.name}: {e}")

        return should_stop

    def _process_container(self, container) -> None:
        """Process a single container for potential stopping."""
        try:
            self._update_container_memory_usage(container)

            if self._should_stop_container(container):
                with FileLock(f"/tmp/{container.name}.lock", timeout=0):
                    container.stop()
                    self.log(f"Stopped inactive container: {container.name}")

        except docker.errors.NotFound:
            self.log(f"Container {container.name} not found when trying to stop")
        except Timeout:
            self.log(f"Could not acquire lock for {container.name}, skipping")
        except Exception as e:
            self.log(f"Unexpected error processing container {container.name}: {e}")

    def _load_existing_stats(self):
        """Load existing container stats from JSON file."""
        if os.path.exists(Config.stats_file):
            with open(Config.stats_file, 'r') as f:
                return json.load(f)
        return {}

    def _save_container_stats(self) -> None:
        if not self.mem_stats:
            return

        with FileLock("/tmp/mem_stats.lock"):
            with open(Config.stats_file, 'w') as f:
                json.dump(self.mem_stats, f, indent=2)
                f.flush()

        # clear any residue
        self.mem_stats = None

    def start(self) -> None:
        """Start the monitoring service."""
        retries = 3
        self.running = True

        # Validate access log path
        if not os.path.exists(Config.access_log_path):
            self.log(f"Access log path {Config.access_log_path} does not exist")

        while self.running:
            try:
                self.sleeping = True
                time.sleep(Config.check_interval_minutes * 60)
                self.sleeping = False

                self.log("Starting Bench Stopper")
                self._init_docker_client()

                running_containers = self.docker_client.containers.list()
                if running_containers:
                    self.mem_stats = self._load_existing_stats()

                for container in running_containers:
                    if not self.running:
                        break

                    self._process_container(container)

                self._save_container_stats()
                retries = 3
            except Exception as e:
                self.log(f"Unexpected error in main loop: {e}")
                time.sleep(60)

                retries -= 1
                if retries <= 0:
                    self.log("Unable to recover - Exiting")
                    break

        self.log("Bench stopper stopped")

    def log(self, message):
        print(f"[{datetime.datetime.now()}] {message!s}", flush=True)


if __name__ == "__main__":
    BenchStopper().start()
