"""
Background Job Scheduler Service.
Provides cron and interval-based recurring job execution for Salesforce Bulk API ingestion,
with manual trigger execution, thread-safe lifecycle controls, and status monitoring.
"""

import time
import threading
import logging
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.utils.constants import SchedulerStatus
from app.utils.exceptions import SchedulerException

logger = logging.getLogger(__name__)

@dataclass
class ScheduledJob:
    """Represents a scheduled ingestion task definition."""
    name: str
    target_object: str
    query: str
    interval_seconds: int
    cron_expr: Optional[str] = None
    enabled: bool = True
    last_run_at: Optional[str] = None
    next_run_epoch: float = field(default_factory=time.time)
    run_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_status: str = "IDLE"

    def is_due(self) -> bool:
        """Check if scheduled execution time has arrived."""
        return self.enabled and time.time() >= self.next_run_epoch

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "target_object": self.target_object,
            "query": self.query,
            "interval_seconds": self.interval_seconds,
            "cron_expr": self.cron_expr,
            "enabled": self.enabled,
            "last_run_at": self.last_run_at,
            "next_run_in_seconds": max(0.0, round(self.next_run_epoch - time.time(), 2)),
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "last_status": self.last_status
        }

class IngestionScheduler:
    """
    Recurring scheduler engine orchestrating periodic Bulk API ingestion jobs.
    """

    def __init__(self, job_executor: Optional[Callable] = None) -> None:
        self.job_executor = job_executor
        self._jobs: Dict[str, ScheduledJob] = {}
        self._status = SchedulerStatus.STOPPED
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._lock = threading.Lock()

    def add_job(
        self,
        name: str,
        target_object: str,
        query: str,
        interval_seconds: int = 300,
        cron_expr: Optional[str] = None,
        enabled: bool = True
    ) -> ScheduledJob:
        """Register a new scheduled ingestion job."""
        with self._lock:
            job = ScheduledJob(
                name=name,
                target_object=target_object,
                query=query,
                interval_seconds=interval_seconds,
                cron_expr=cron_expr,
                enabled=enabled,
                next_run_epoch=time.time() + interval_seconds
            )
            self._jobs[name] = job
            logger.info(f"Registered scheduled job '{name}' (every {interval_seconds}s)")
            return job

    def remove_job(self, name: str) -> bool:
        """Remove a scheduled job by name."""
        with self._lock:
            if name in self._jobs:
                del self._jobs[name]
                logger.info(f"Removed scheduled job '{name}'")
                return True
            return False

    def trigger_job_now(self, name: str) -> Dict[str, Any]:
        """
        Manually trigger immediate execution of a registered job.
        Bypasses next_run_epoch check.
        """
        with self._lock:
            job = self._jobs.get(name)
            if not job:
                raise SchedulerException(f"Scheduled job '{name}' not found.")

        logger.info(f"Manually triggering scheduled job '{name}' immediately...")
        return self._execute_job(job)

    def _execute_job(self, job: ScheduledJob) -> Dict[str, Any]:
        """Execute a job and update runtime execution counters."""
        job.run_count += 1
        job.last_run_at = datetime.now(timezone.utc).isoformat()
        job.next_run_epoch = time.time() + job.interval_seconds

        result = {}
        try:
            if self.job_executor:
                result = self.job_executor(job.target_object, job.query)
            job.success_count += 1
            job.last_status = "SUCCESS"
            logger.info(f"Scheduled job '{job.name}' executed successfully.")
        except Exception as exc:
            job.failure_count += 1
            job.last_status = f"FAILED: {str(exc)}"
            logger.error(f"Scheduled job '{job.name}' execution failed: {str(exc)}", exc_info=True)
            result = {"error": str(exc)}

        return {
            "name": job.name,
            "status": job.last_status,
            "run_count": job.run_count,
            "result": result
        }

    def start(self) -> None:
        """Start the background scheduler thread."""
        with self._lock:
            if self._status == SchedulerStatus.RUNNING:
                return
            self._status = SchedulerStatus.RUNNING
            self._stop_event.clear()
            self._pause_event.clear()

            self._thread = threading.Thread(
                target=self._scheduler_loop,
                name="IngestionScheduler-Daemon",
                daemon=True
            )
            self._thread.start()
            logger.info("IngestionScheduler daemon started.")

    def pause(self) -> None:
        """Pause scheduler execution without stopping thread."""
        with self._lock:
            self._status = SchedulerStatus.PAUSED
            self._pause_event.set()
            logger.info("IngestionScheduler execution paused.")

    def resume(self) -> None:
        """Resume paused scheduler execution."""
        with self._lock:
            self._status = SchedulerStatus.RUNNING
            self._pause_event.clear()
            logger.info("IngestionScheduler execution resumed.")

    def stop(self, wait: bool = True, timeout: float = 3.0) -> None:
        """Stop scheduler thread gracefully."""
        with self._lock:
            if self._status == SchedulerStatus.STOPPED:
                return
            self._status = SchedulerStatus.STOPPED
            self._stop_event.set()
            self._pause_event.clear()
            logger.info("Stopping IngestionScheduler daemon...")

        if wait and self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

        logger.info("IngestionScheduler daemon stopped.")

    def _scheduler_loop(self) -> None:
        """Continuous periodic polling loop checking for due jobs."""
        while not self._stop_event.is_set():
            if self._pause_event.is_set():
                time.sleep(0.5)
                continue

            # Check for due jobs
            due_jobs = []
            with self._lock:
                for job in self._jobs.values():
                    if job.is_due():
                        due_jobs.append(job)

            for job in due_jobs:
                if self._stop_event.is_set():
                    break
                self._execute_job(job)

            time.sleep(1.0)

    def get_status(self) -> Dict[str, Any]:
        """Return scheduler health and registered jobs snapshot."""
        with self._lock:
            return {
                "status": self._status.value,
                "is_running": self._status == SchedulerStatus.RUNNING,
                "is_paused": self._status == SchedulerStatus.PAUSED,
                "job_count": len(self._jobs),
                "jobs": [j.to_dict() for j in self._jobs.values()]
            }

