"""
Enterprise Telemetry and Operational Metrics Collector.
Tracks rows processed, job state transitions, processing throughput (rows/sec),
API and polling latencies, and process heap memory consumption.
"""

import time
import threading
import os
import psutil
from typing import Dict, Any, List

class MetricsCollector:
    """
    Thread-safe operational telemetry collector for ingestion pipelines.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(MetricsCollector, cls).__new__(cls)
                cls._instance._init_metrics()
            return cls._instance

    def _init_metrics(self) -> None:
        self.start_time = time.time()
        self.total_jobs_started = 0
        self.total_rows_processed = 0
        self.total_jobs_completed = 0
        self.total_jobs_failed = 0
        self.total_jobs_aborted = 0
        self.retry_count = 0
        self.active_workers_count = 0
        self.queue_size = 0
        self.dlq_size = 0
        self.polling_latencies: List[float] = []
        self.api_latencies: List[float] = []
        self.active_jobs_count = 0
        self.last_api_usage_header = "N/A"

    def record_rows_processed(self, count: int) -> None:
        with self._lock:
            self.total_rows_processed += count

    def record_job_completed(self) -> None:
        with self._lock:
            self.total_jobs_completed += 1
            if self.active_jobs_count > 0:
                self.active_jobs_count -= 1

    def record_job_failed(self) -> None:
        with self._lock:
            self.total_jobs_failed += 1
            if self.active_jobs_count > 0:
                self.active_jobs_count -= 1

    def record_job_started(self) -> None:
        with self._lock:
            self.total_jobs_started += 1
            self.active_jobs_count += 1

    def record_retry(self) -> None:
        with self._lock:
            self.retry_count += 1

    def set_active_workers(self, count: int) -> None:
        with self._lock:
            self.active_workers_count = count

    def set_queue_size(self, size: int) -> None:
        with self._lock:
            self.queue_size = size

    def set_dlq_size(self, size: int) -> None:
        with self._lock:
            self.dlq_size = size

    def record_polling_latency(self, latency_ms: float) -> None:
        with self._lock:
            self.polling_latencies.append(latency_ms)
            if len(self.polling_latencies) > 100:
                self.polling_latencies.pop(0)

    def record_api_latency(self, latency_ms: float) -> None:
        with self._lock:
            self.api_latencies.append(latency_ms)
            if len(self.api_latencies) > 100:
                self.api_latencies.pop(0)

    def set_api_usage(self, usage_header: str) -> None:
        with self._lock:
            self.last_api_usage_header = usage_header

    def get_memory_usage_mb(self) -> float:
        """Get current resident set size (RSS) memory in megabytes."""
        try:
            process = psutil.Process(os.getpid())
            return round(process.memory_info().rss / (1024 * 1024), 2)
        except Exception:
            return 0.0

    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """Generate a complete telemetry metrics snapshot."""
        with self._lock:
            uptime = max(0.001, time.time() - self.start_time)
            avg_polling_lat = (
                round(sum(self.polling_latencies) / len(self.polling_latencies), 2)
                if self.polling_latencies else 0.0
            )
            avg_api_lat = (
                round(sum(self.api_latencies) / len(self.api_latencies), 2)
                if self.api_latencies else 0.0
            )
            processing_speed = round(self.total_rows_processed / uptime, 2)

            return {
                "uptime_seconds": round(uptime, 2),
                "total_jobs": self.total_jobs_started,
                "active_jobs": self.active_jobs_count,
                "jobs_completed": self.total_jobs_completed,
                "successful_jobs": self.total_jobs_completed,
                "jobs_failed": self.total_jobs_failed,
                "failed_jobs": self.total_jobs_failed,
                "retry_count": self.retry_count,
                "active_workers": self.active_workers_count,
                "queue_size": self.queue_size,
                "dlq_size": self.dlq_size,
                "total_rows_processed": self.total_rows_processed,
                "processing_speed_rows_per_sec": processing_speed,
                "avg_polling_latency_ms": avg_polling_lat,
                "avg_api_latency_ms": avg_api_lat,
                "process_memory_usage_mb": self.get_memory_usage_mb(),
                "salesforce_governor_limits": self.last_api_usage_header
            }

    def export_prometheus_metrics(self) -> str:
        """Generate Prometheus exposition format metrics string."""
        snapshot = self.get_metrics_snapshot()
        lines = [
            "# HELP sf_ingest_uptime_seconds Total runtime of the ingestion application in seconds",
            "# TYPE sf_ingest_uptime_seconds gauge",
            f"sf_ingest_uptime_seconds {snapshot['uptime_seconds']}",
            "# HELP sf_ingest_total_jobs_total Total bulk ingestion jobs started",
            "# TYPE sf_ingest_total_jobs_total counter",
            f"sf_ingest_total_jobs_total {snapshot['total_jobs']}",
            "# HELP sf_ingest_successful_jobs_total Total successfully processed bulk jobs",
            "# TYPE sf_ingest_successful_jobs_total counter",
            f"sf_ingest_successful_jobs_total {snapshot['successful_jobs']}",
            "# HELP sf_ingest_failed_jobs_total Total failed bulk jobs",
            "# TYPE sf_ingest_failed_jobs_total counter",
            f"sf_ingest_failed_jobs_total {snapshot['failed_jobs']}",
            "# HELP sf_ingest_retry_count_total Total retry attempts dispatched",
            "# TYPE sf_ingest_retry_count_total counter",
            f"sf_ingest_retry_count_total {snapshot['retry_count']}",
            "# HELP sf_ingest_active_workers Current active worker threads",
            "# TYPE sf_ingest_active_workers gauge",
            f"sf_ingest_active_workers {snapshot['active_workers']}",
            "# HELP sf_ingest_queue_size Current queue pending items",
            "# TYPE sf_ingest_queue_size gauge",
            f"sf_ingest_queue_size {snapshot['queue_size']}",
            "# HELP sf_ingest_dlq_size Current dead-letter queue count",
            "# TYPE sf_ingest_dlq_size gauge",
            f"sf_ingest_dlq_size {snapshot['dlq_size']}",
            "# HELP sf_ingest_rows_processed_total Total records ingested into database",
            "# TYPE sf_ingest_rows_processed_total counter",
            f"sf_ingest_rows_processed_total {snapshot['total_rows_processed']}",
            "# HELP sf_ingest_memory_usage_mb Resident set size process memory in megabytes",
            "# TYPE sf_ingest_memory_usage_mb gauge",
            f"sf_ingest_memory_usage_mb {snapshot['process_memory_usage_mb']}",
            ""
        ]
        return "\n".join(lines)

# Global singleton collector
metrics_collector = MetricsCollector()
