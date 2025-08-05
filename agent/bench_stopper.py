import os
import time
import datetime
import signal
import json
from pathlib import Path

import docker
from filelock import FileLock, Timeout


class Config:
    check_interval_minutes: int = 15
    inactive_threshold_hours: int = 3
    min_uptime_hours: int = 3
    access_log_path: str = "/var/log/nginx"
    lock_timeout_seconds: int = 0
    stats_file: str = "/home/frappe/agent/bench-memory-stats.json"


class BenchStopper:
    """Monitors Docker containers based on nginx access log modification times."""

    def __init__(self):
        self.running = False
        self.docker_client = None
        self.mem_stats = {}
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.log(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _init_docker_client(self) -> docker.DockerClient:
        try:
            self.docker_client = docker.from_env()
            self.docker_client.ping()
        except Exception as e:
            self.log(f"Failed to connect to Docker: {e}")
            raise

        return self.docker_client

    def _update_container_memory_usage(self, container):
        """Get container memory usage statistics."""
        try:
            stats = container.stats(stream=False)
            memory_stats = stats.get('memory_stats', {})

            if 'usage' in memory_stats:
                usage_bytes = memory_stats['usage']
                usage_mb = usage_bytes / (1024 * 1024)
                self.mem_stats[container.name] = usage_mb
        except Exception as e:
            self.log(f"Could not get memory stats for {container.name}: {e}")


    def _find_log_files_for_bench(self, bench_name: str):
        """Find nginx log files associated with a bench."""
        log_files = []
        access_log_dir = Path(Config.access_log_path)

        if not access_log_dir.exists():
            return

        # Look for log files that might be associated with this bench
        for pattern in [
            f"{bench_name}*.log",
            f"{bench_name.replace('-', '_')}*.log",
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
                start_time_str.rstrip('Z')
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

            with FileLock(f"/tmp/{container.name}.lock", timeout=Config.lock_timeout_seconds):
                if self._should_stop_container(container):
                    container.stop()
                    self.log(f"Stopping inactive container: {container.name}")

        except docker.errors.NotFound:
            self.log(f"Container {container.name} not found when trying to stop")
        except docker.errors.APIError as e:
            self.log(f"Docker API error: {e}")
        except Timeout:
            self.log(f"Could not acquire lock for {container.name}, skipping")
        except Exception as e:
            self.log(f"Unexpected error processing container {container.name}: {e}")

    def _load_existing_stats():
        """Load existing container stats from JSON file."""
        if os.path.exists(Config.stats_file):
            with open(Config.stats_file, 'r') as f:
                return json.load(f)
        return {}

    def _save_container_stats(self) -> None:
        with FileLock("/tmp/mem_stats.lock"):
            stats = self._load_existing_stats()
            stats.update(self.mem_stats)

            with open(Config.stats_file, 'w') as f:
                json.dump(stats, f, indent=2)

        # clear any residue
        self.mem_stats = {}

    def start(self) -> None:
        """Start the monitoring service."""
        self.log("Starting Docker container monitor")

        retries = 3
        self.running = True

        # Validate access log path
        if not os.path.exists(Config.access_log_path):
            self.log(f"Access log path {Config.access_log_path} does not exist")

        while self.running:
            try:
                time.sleep(Config.check_interval_minutes * 60)

                self._init_docker_client()

                running_containers = self.docker_client.containers.list()
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
        print(f"[{datetime.datetime.now()}] {message!s}")


if __name__ == "__main__":
    BenchStopper().start()
