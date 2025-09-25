"""
Bench Container Starter

This service monitors a Redis queue for bench start requests and starts containers
in batches while respecting server memory limits and container memory requirements.
"""

import sys
import os
import time
import json
import signal
import psutil
import datetime
from filelock import FileLock, Timeout

import redis
import docker
from agent.server import Server
from agent.bench import Bench


class Config:
    redis_queue_key: str = "bench_start_queue"
    redis_failed_hash_key: str = "bench_start_failed"
    redis_failed_hash_expiry_mins: int = 10
    check_interval_seconds: int = 60
    memory_reserve_percent: float = 35.0  # Reserve 35% of system memory
    memory_stats_file: str = "/home/frappe/agent/bench-memory-stats.json"
    worker_memory_mb: int = 100  # this is an estimate
    batch_size: int = 5
    nginx_log_dir: str = "/var/log/nginx"
    activity_threshold_hours: float = 1.0  # Consider container active if accessed within 1 hour
    available_memory_adjustment_percent: float = 20.0  # Max 20% of available memory for adjustments



class BenchStarter:
    def __init__(self):
        self.redis_client = None
        self.docker_client = None
        self.running = False
        self.sleeping = False

    def queue_request(self, bench_name: str, ignore_throttle: bool=False):
        if not self.redis_client:
            self._init_redis_client()

        # TODO: add an enum for status
        status = "REQUEST_ALREADY_EXISTS"
        queue_items = self.redis_client.lrange(Config.redis_queue_key, 0, -1)
        if bench_name not in queue_items:
            status = "THROTTLED"
            if ignore_throttle or not self.redis_client.hget(f"{Config.redis_failed_hash_key}:{bench_name}", "throttle"):
                self.redis_client.rpush(Config.redis_queue_key, bench_name)
                status = "QUEUED"

        return status

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

    def _init_redis_client(self):
        try:
            self.redis_client = redis.Redis(
                port=Server().config["redis_port"],
                decode_responses=True
            )
        except Exception as e:
            self.log(f"Failed to connect to Redis: {e}")
            raise

    def _init_docker_client(self):
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            self.log(f"Failed to connect to Docker: {e}")
            raise

    def _get_system_memory_info(self):
        """Get system memory information including swap."""
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()

        # Calculate effective memory (RAM + swap)
        total_effective_memory = memory.total + swap.total
        available_effective_memory = memory.available + (swap.total - swap.used)

        return {
            'total_bytes': memory.total,
            'available_bytes': memory.available,
            'swap_total_bytes': swap.total,
            'swap_used_bytes': swap.used,
            'swap_available_bytes': swap.total - swap.used,
            'total_effective_bytes': total_effective_memory,
            'available_effective_bytes': available_effective_memory,
        }

    def _get_memory_threshold(self):
        # Use effective memory (RAM + swap) for threshold calculation
        total_effective_memory = self._get_system_memory_info()['total_effective_bytes']
        return int(total_effective_memory * (Config.memory_reserve_percent / 100))

    def _load_memory_stats(self):
        """Load memory stats from the stats file."""
        try:
            with FileLock("/tmp/mem_stats.lock"):
                if os.path.exists(Config.memory_stats_file):
                    with open(Config.memory_stats_file, 'r') as f:
                        return json.load(f)
        except Exception as e:
            self.log(f"Could not load memory stats file: {e}")
        return {}

    def _load_bench_config(self, bench_name: str):
        """Load bench configuration from config.json."""
        try:
            return Bench(bench_name, Server()).bench_config
        except Exception as e:
            self.log(f"Could not load config for bench {bench_name}: {e}")
        return {}

    def _calculate_predictive_memory_adjustment(self, bench_name: str) -> int:
        """Calculate memory adjustment based on prediction accuracy and activity."""
        try:
            # Check if container is active
            if not self._is_container_active(bench_name):
                self.log(f"{bench_name} inactive (no recent web activity), skipping adjustment")
                return 0

            # Get current memory usage of the container if it's running
            current_usage = self._get_current_container_memory(bench_name)
            if current_usage <= 0:
                # Container not running or can't get stats
                return 0

            avg = self.container_mem_stats[bench_name]

            # underestimation (how much more memory container uses than current)
            return max(0, avg - current_usage)

        except Exception as e:
            self.log(f"Error calculating predictive adjustment for {bench_name}: {e}")

        return 0

    def _get_adjusted_available_memory(self) -> int:
        """Adjust available memory based on historical data of running containers."""

        total_adjustment = 0
        for bench in self.docker_client.containers.list():
            if bench.name not in self.container_mem_stats:
                continue  # No historical data to work with

            total_adjustment += self._calculate_predictive_memory_adjustment(bench.name)

        available_memory = self._get_system_memory_info()["available_effective_bytes"]
        max_adjustment = int(available_memory * (Config.available_memory_adjustment_percent / 100))
        if total_adjustment >= max_adjustment:
            total_adjustment = max_adjustment

        return available_memory - total_adjustment

    def _calculate_bench_memory_requirement(self, bench_name: str):
        # First try to get from memory stats file
        if bench_name in self.container_mem_stats:
            mem_stat = self.container_mem_stats[bench_name]
            if mem_stat > 0:
                self.log(f"Using historical memory for {bench_name}: {mem_stat/(1024*1024)}MB")
                return mem_stat

        # use bench config to figure out the memory - this is an estimate at best
        bench_config = self._load_bench_config(bench_name)

        # Calculate based on workers
        background_workers = bench_config.get('background_workers', 1)
        gunicorn_workers = bench_config.get('gunicorn_workers', 2)
        merge_all_rq = bench_config.get('merge_all_rq_queues', False)
        merge_default_short = bench_config.get('merge_default_and_short_rq_queues', False)

        # Calculate total background workers based on queue merging
        if merge_all_rq:
            total_bg_workers = background_workers
        elif merge_default_short:
            total_bg_workers = 2 * background_workers
        else:
            total_bg_workers = 3 * background_workers

        total_workers = gunicorn_workers + total_bg_workers
        return Config.worker_memory_mb * total_workers * 1024 * 1024

    def _can_start_bench(self, available_memory: int, min_available_threshold: int, required_memory_by_bench: int):
        # Check if current available memory is already below threshold
        if available_memory < min_available_threshold:
            return False, "Available memory below minimum threshold"

        # Check if starting this bench would drop available memory below threshold
        projected_available = available_memory - required_memory_by_bench
        if projected_available < min_available_threshold:
            return False, "Starting bench would drop available memory below threshold"

        return True, ""

    def _start_container(self, bench_name: str):
        try:
            with FileLock(f"/tmp/{bench_name}.lock", timeout=60):
                container = self.docker_client.containers.get(bench_name)
                if container.status in ("exited", "stopped"):
                    container.start()
                    self.log(f"Started container {bench_name}")

            return True
        except docker.errors.NotFound:
            self.log(f"Container {bench_name} not found")
        except Timeout:
            self.log(f"Could not acquire lock for {bench_name}, skipping")
        except Exception as e:
            self.log(f"Failed to start container {bench_name}: {e}")

        return False

    def _get_pending_benches(self):
        """Get list of benches waiting to be started from Redis list."""
        try:
            # Get items from list (FIFO order)
            pending_items = self.redis_client.lrange(
                Config.redis_queue_key,
                0,
                Config.batch_size - 1  # Get only batch_size items
            )

            return [item.strip() for item in pending_items]
        except Exception as e:
            self.log(f"Error getting pending benches from Redis: {e}")

        return []

    def _remove_from_queue(self, bench_name: str, pipe=None):
        """Remove bench from the Redis queue after successful start."""
        try:
            client = pipe or self.redis_client
            client.lrem(Config.redis_queue_key, 1, bench_name)
        except Exception as e:
            self.log(f"Error removing {bench_name} from start queue: {e}")

    def _remove_and_add_failed_status(self, bench_name: str, info: str, throttle: bool=False):
        """Atomically remove bench from start queue and add failed status."""
        try:
            with self.redis_client.pipeline() as pipe:
                pipe.multi()

                self._remove_from_queue(bench_name, pipe)

                failed_key = f"{Config.redis_failed_hash_key}:{bench_name}"
                pipe.hset(failed_key, mapping={'info': info, 'throttle': int(throttle)})
                pipe.expire(failed_key, Config.redis_failed_hash_expiry_mins * 60) # set expiry

                # Execute all commands atomically
                pipe.execute()
        except Exception as e:
            self.log(f"Error moving {bench_name} to failed queue: {e}")

    def _process_batch(self):
        # Get pending benches from main queue
        pending_benches = self._get_pending_benches()
        if pending_benches:
            self.container_mem_stats = self._load_memory_stats()

            # Get memory state
            min_available_threshold = self._get_memory_threshold()
            available_memory = self._get_adjusted_available_memory()

        for bench_name in pending_benches:
            if not self.running:
                break

            self.log(f"Processing {bench_name}")
            required_memory_by_bench = self._calculate_bench_memory_requirement(bench_name)

            throttle = True
            # Check if we can start this bench
            can_start, info = self._can_start_bench(available_memory, min_available_threshold, required_memory_by_bench)
            if can_start:
                if self._start_container(bench_name):
                    # reduce the available memory
                    available_memory = available_memory - required_memory_by_bench
                    self._remove_from_queue(bench_name)
                    continue

                # dont throttle for non-memory related stuff
                throttle = False
                info = "Please try to queue again and/or contact support."

            self._remove_and_add_failed_status(bench_name, info, throttle)
            self.log(f"Cannot start {bench_name}: {info}")

    def start(self):
        """Start the bench starter service."""
        self._setup_signal_handlers()

        retries = 3
        self.running = True
        while self.running:
            try:
                self.sleeping = True
                time.sleep(Config.check_interval_seconds)
                self.sleeping = False

                self.log("Starting Bench Container Starter")

                self._init_redis_client()
                self._init_docker_client()
                self._process_batch()

                retries = 3
            except Exception as e:
                self.log(f"Unexpected error: {e}")

                retries -= 1
                if retries < 0:
                    self.log("Unable to recover - Exiting")
                    break

        self.log("Bench Starter stopped")

    def log(self, message):
        print(f"[{datetime.datetime.now()}] {message!s}", flush=True)


if __name__ == "__main__":
    BenchStarter().start()
