"""
Enterprise Monitoring & Dashboard Service.
Aggregates real-time telemetry, worker pool utilization, retry queues, Dead Letter Queue (DLQ),
scheduler status, database connectivity, and provides Prometheus metrics and liveness/readiness probes.
"""

import time
import logging
from typing import Dict, Any, Optional

from app.database.database import check_db_health
from app.utils.telemetry import metrics_collector
from app.services.worker_pool import IngestionWorkerPool
from app.services.retry_queue import RetryQueue
from app.services.dlq_service import DeadLetterQueue
from app.services.scheduler_service import IngestionScheduler
from app.integrations.salesforce_client import SalesforceClient

logger = logging.getLogger(__name__)

class MonitoringService:
    """
    Central monitoring and diagnostic aggregator.
    """

    def __init__(
        self,
        worker_pool: Optional[IngestionWorkerPool] = None,
        retry_queue: Optional[RetryQueue] = None,
        dlq: Optional[DeadLetterQueue] = None,
        scheduler: Optional[IngestionScheduler] = None,
        salesforce_client: Optional[SalesforceClient] = None
    ) -> None:
        self.worker_pool = worker_pool
        self.retry_queue = retry_queue
        self.dlq = dlq
        self.scheduler = scheduler
        self.salesforce_client = salesforce_client

    def get_dashboard_summary(self) -> Dict[str, Any]:
        """
        Generate comprehensive enterprise operational dashboard summary.
        Combines telemetry counters, worker pool stats, queue depths, DLQ items,
        scheduler status, and infrastructure health.
        """
        metrics_snapshot = metrics_collector.get_metrics_snapshot()
        db_health = check_db_health()

        worker_stats = self.worker_pool.get_stats() if self.worker_pool else {
            "max_workers": 4, "active_workers": 0, "queue_size": 0, "is_running": False
        }

        retry_items = self.retry_queue.peek_all() if self.retry_queue else []
        dlq_items = self.dlq.list_items(limit=10) if self.dlq else []
        scheduler_status = self.scheduler.get_status() if self.scheduler else {"status": "NOT_CONFIGURED"}

        # Calculate throughput statistics
        uptime = metrics_snapshot.get("uptime_seconds", 1.0)
        total_rows = metrics_snapshot.get("total_rows_processed", 0)
        jobs_completed = metrics_snapshot.get("jobs_completed", 0)

        throughput = {
            "rows_per_second": round(total_rows / max(1.0, uptime), 2),
            "jobs_per_minute": round((jobs_completed / max(1.0, uptime)) * 60.0, 2),
            "avg_polling_latency_ms": metrics_snapshot.get("avg_polling_latency_ms", 0.0),
            "avg_api_latency_ms": metrics_snapshot.get("avg_api_latency_ms", 0.0)
        }

        return {
            "system": {
                "status": "UP" if db_health.get("status") == "UP" else "DEGRADED",
                "uptime_seconds": uptime,
                "memory_rss_mb": metrics_snapshot.get("process_memory_usage_mb", 0.0),
                "timestamp": time.time()
            },
            "job_metrics": {
                "total_jobs": metrics_snapshot.get("total_jobs", 0),
                "active_jobs": metrics_snapshot.get("active_jobs", 0),
                "successful_jobs": metrics_snapshot.get("successful_jobs", 0),
                "failed_jobs": metrics_snapshot.get("failed_jobs", 0),
                "retry_count": metrics_snapshot.get("retry_count", 0),
                "total_records_ingested": total_rows
            },
            "throughput": throughput,
            "worker_pool": worker_stats,
            "retry_queue": {
                "size": len(retry_items),
                "pending_tasks": retry_items[:5]
            },
            "dead_letter_queue": {
                "size": self.dlq.size() if self.dlq else 0,
                "recent_failures": dlq_items
            },
            "scheduler": scheduler_status,
            "infrastructure": {
                "database": db_health,
                "salesforce_governor_limits": metrics_snapshot.get("salesforce_governor_limits", "N/A")
            }
        }

    def get_readiness_probe(self) -> Dict[str, Any]:
        """
        Kubernetes/Cloud readiness probe.
        Checks if the service is ready to accept and process traffic:
        - Database connection active
        - Worker pool alive and queue not full
        - Salesforce client initialized
        """
        db_health = check_db_health()
        db_ok = db_health.get("status") == "UP"

        worker_ok = True
        if self.worker_pool:
            worker_ok = self.worker_pool.is_running() and (self.worker_pool.queue_size() < self.worker_pool.max_queue_size)

        sf_ok = True
        try:
            if self.salesforce_client:
                sf_ok = bool(self.salesforce_client.instance_url)
        except Exception:
            sf_ok = False

        is_ready = db_ok and worker_ok and sf_ok

        return {
            "status": "READY" if is_ready else "NOT_READY",
            "ready": is_ready,
            "checks": {
                "database": {"status": "UP" if db_ok else "DOWN", "details": db_health},
                "worker_pool": {"status": "UP" if worker_ok else "DEGRADED", "queue_available": worker_ok},
                "salesforce_client": {"status": "INITIALIZED" if sf_ok else "NOT_CONFIGURED"}
            }
        }

    def get_liveness_probe(self) -> Dict[str, Any]:
        """
        Kubernetes/Cloud liveness probe.
        Checks if the process is alive, responding, and healthy.
        """
        memory_mb = metrics_collector.get_memory_usage_mb()
        uptime = round(time.time() - metrics_collector.start_time, 2)

        return {
            "status": "ALIVE",
            "alive": True,
            "uptime_seconds": uptime,
            "memory_usage_mb": memory_mb
        }

    def get_prometheus_metrics(self) -> str:
        """Return Prometheus text exposition format metrics."""
        return metrics_collector.export_prometheus_metrics()

