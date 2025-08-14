"""
Bench Container Starter

This service monitors a Redis queue for bench start requests and starts containers
in batches while respecting server memory limits and container memory requirements.
"""

import os
import time
import json
import signal
import psutil
import datetime
from typing import Dict, List
from filelock import FileLock

import redis
import docker
from agent.server import Server
from agent.bench import Bench


class Config:
    redis_port: int = Server().config["redis_port"]
    redis_queue_key: str = "bench_start_queue"
    redis_failed_hash_key: str = "bench_start_failed"
    redis_failed_hash_expiry_mins: int = 15
    check_interval_seconds: int = 60
    memory_reserve_percent: float = 25.0  # Reserve 25% of system memory
    memory_stats_file: str = "/home/frappe/agent/bench-memory-stats.json"
    worker_memory_mb: int = 100  # this is an estimate
    batch_size: int = 5


class BenchStarter:
    def __init__(self):
        self.redis_client = None
        self.docker_client = None
        self.running = False
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.log(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _init_redis_client(self):
        """Get or create Redis client with error handling."""
        try:
            self.redis_client = redis.Redis(
                port=Config.redis_port,
                decode_responses=True
            )
        except Exception as e:
            self.log(f"Failed to connect to Redis: {e}")
            raise

    def _init_docker_client(self):
        """Get or create Docker client with error handling."""
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            self.log(f"Failed to connect to Docker: {e}")
            raise

    def _get_system_memory_info(self) -> Dict[str, int]:
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

    def _get_memory_threshold_bytes(self, memory_info) -> int:
        """Calculate minimum available memory threshold (25% of total effective memory)."""
        # Use effective memory (RAM + swap) for threshold calculation
        total_effective_memory = memory_info['total_effective_bytes']
        # We want to maintain at least 25% of total memory as available
        min_available_threshold = int(total_effective_memory * (Config.memory_reserve_percent / 100))
        return min_available_threshold

    def _load_memory_stats(self) -> Dict[str, Dict]:
        """Load memory stats from the stats file."""
        try:
            with FileLock("/tmp/mem_stats.lock"):
                if os.path.exists(Config.memory_stats_file):
                    with open(Config.memory_stats_file, 'r') as f:
                        return json.load(f)
        except Exception as e:
            self.log(f"Could not load memory stats file: {e}")
        return {}

    def _load_bench_config(self, bench_name: str) -> Dict:
        """Load bench configuration from config.json."""
        try:
            return Bench(bench_name, Server()).bench_config
        except Exception as e:
            self.log(f"Could not load config for bench {bench_name}: {e}")
        return {}

    def _calculate_bench_memory_requirement(self, bench_name: str) -> int:
        # First try to get from memory stats file
        if bench_name in self.container_mem_stats:
            mem_stat = self.container_mem_stats[bench_name]
            if mem_stat > 0:
                self.log(f"Using historical memory for {bench_name}: {mem_stat}MB")
                return mem_stat * 1024 * 1024

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
        """Check if there's enough memory to start a bench."""
        # TODO: we could add a slack - like say if it's upto 100mb/3% above then let the bench start (?)

        # Check if current available memory is already below threshold
        if available_memory < min_available_threshold:
            return False, "Available memory below minimum threshold."

        # Check if starting this bench would drop available memory below threshold
        projected_available = available_memory - required_memory_by_bench
        if projected_available < min_available_threshold:
            return False, "Starting bench would drop available memory below threshold."

        return True, ""

    def _start_container(self, bench_name: str) -> bool:
        """Start a container for the given bench."""
        try:
            with FileLock(f"/tmp/{bench_name}.lock"):
                container = self.docker_client.containers.get(bench_name)
                if container.status in ("exited", "stopped"):
                    container.start()
                    self.log(f"Started container {bench_name}")

            return True
        except docker.errors.NotFound:
            self.log(f"Container {bench_name} not found")
        except Exception as e:
            self.log(f"Failed to start container {bench_name}: {e}")

        return False

    def _get_pending_benches(self) -> List[str]:
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

    def _remove_from_queue(self, bench_name: list) -> bool:
        """Remove bench from the Redis queue after successful start."""
        try:
            self.redis_client.lrem(Config.redis_queue_key, 1, bench_name)
        except Exception as e:
            self.log(f"Error removing {bench_name} from start queue: {e}")

    def _add_to_failed_queue(self, bench_name: str, reason: str) -> bool:
        """Add a bench to the failed queue with expiry."""
        try:
            failed_entry = {
                'reason': reason,
            }

            failed_key = f"{Config.redis_failed_hash_key}:{bench_name}"
            self.redis_client.hset(failed_key, mapping=failed_entry)

            # set expiry
            expiry_seconds = Config.redis_failed_hash_expiry_mins * 60
            self.redis_client.expire(failed_key, expiry_seconds)
            return True
        except Exception as e:
            self.log(f"Error adding {bench_name} to failed queue: {e}")

        return False

    def _process_batch(self) -> None:
        # Get current memory state
        memory_info = self._get_system_memory_info()
        min_available_threshold = self._get_memory_threshold_bytes(memory_info)
        available_memory = memory_info["available_effective_bytes"]

        # Get pending benches from main queue
        pending_benches = self._get_pending_benches()
        for i, bench_name in enumerate(pending_benches):
            if not self.running:
                break

            self.log(f"Processing {bench_name}")
            required_memory_by_bench = self._calculate_bench_memory_requirement(bench_name)

            # Check if we can start this bench
            can_start, reason = self._can_start_bench(available_memory, min_available_threshold, required_memory_by_bench)
            if can_start:
                if self._start_container(bench_name):
                    # reduce the available memory
                    available_memory = available_memory - required_memory_by_bench
                    self._remove_from_queue(bench_name)
                    continue

                reason = "Failed to start container. Please try to queue again and/or contact support."

            self._remove_from_queue(bench_name)
            self._add_to_failed_queue(bench_name, reason)
            self.log(f"Cannot start {bench_name}: {reason}")

    def start(self) -> None:
        """Start the bench starter service."""
        retries = 3
        self.running = True

        while self.running:
            try:
                time.sleep(Config.check_interval_seconds)
                self.log("Starting Bench Container Starter")

                self.container_mem_stats = self._load_memory_stats()
                self._init_redis_client()
                self._init_docker_client()

                self._process_batch()
                retries = 3
            except Exception as e:
                self.log(f"Unexpected error: {e}")
                time.sleep(60)  # Wait a minute before retrying

                retries -= 1
                if retries < 0:
                    self.log("Unable to recover")
                    break

        self.log("Bench Starter stopped")

    def log(self, message):
        print(f"[{datetime.datetime.now()}] {message!s}", flush=True)


if __name__ == "__main__":
    BenchStarter().start()
