"""
Retry Queue Service.
Implements thread-safe in-memory scheduled retry tracking with Exponential Backoff
and Decorrelated Jitter for transient Bulk API ingestion failures.
"""

import time
import uuid
import threading
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.utils.resilience import compute_decorrelated_jitter
from app.utils.telemetry import metrics_collector

logger = logging.getLogger(__name__)

@dataclass
class RetryTask:
    """Represents a queued retry job item."""
    task_id: str
    job_id: str
    target_object: str
    query: str
    attempt: int
    max_retries: int
    delay_seconds: float
    scheduled_at: float  # epoch seconds when ready
    last_error: str
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    payload: Dict[str, Any] = field(default_factory=dict)

    def is_ready(self) -> bool:
        """Check if scheduled backoff duration has elapsed."""
        return time.time() >= self.scheduled_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "job_id": self.job_id,
            "target_object": self.target_object,
            "query": self.query,
            "attempt": self.attempt,
            "max_retries": self.max_retries,
            "delay_seconds": self.delay_seconds,
            "scheduled_at": self.scheduled_at,
            "remaining_seconds": max(0.0, round(self.scheduled_at - time.time(), 2)),
            "last_error": self.last_error,
            "created_at": self.created_at
        }

class RetryQueue:
    """
    Thread-safe retry queue manager for scheduled re-execution.
    """

    def __init__(
        self,
        base_delay: float = 2.0,
        max_delay: float = 60.0,
        max_retries: int = 3
    ) -> None:
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self._tasks: Dict[str, RetryTask] = {}
        self._lock = threading.Lock()

    def enqueue(
        self,
        job_id: str,
        target_object: str,
        query: str,
        error: Exception,
        attempt: int = 1,
        payload: Optional[Dict[str, Any]] = None
    ) -> Optional[RetryTask]:
        """
        Schedule a job for retry if attempt <= max_retries.
        Calculates next retry delay with Decorrelated Jitter.
        """
        if attempt > self.max_retries:
            logger.warning(
                f"Job {job_id} exceeded max retry limit ({attempt}/{self.max_retries}). "
                f"Cannot enqueue to Retry Queue."
            )
            return None

        # Calculate backoff with Decorrelated Jitter
        current_delay = self.base_delay * (2 ** (attempt - 1))
        jittered_delay = compute_decorrelated_jitter(
            base_delay=self.base_delay,
            current_delay=current_delay,
            max_delay=self.max_delay
        )
        scheduled_time = time.time() + jittered_delay

        task = RetryTask(
            task_id=str(uuid.uuid4()),
            job_id=job_id,
            target_object=target_object,
            query=query,
            attempt=attempt,
            max_retries=self.max_retries,
            delay_seconds=jittered_delay,
            scheduled_at=scheduled_time,
            last_error=str(error),
            payload=payload or {}
        )

        with self._lock:
            self._tasks[task.task_id] = task

        metrics_collector.record_retry()
        logger.info(
            f"Enqueued Job {job_id} for Retry #{attempt}/{self.max_retries} "
            f"in {jittered_delay}s. Error: {str(error)}"
        )
        return task

    def get_ready_tasks(self) -> List[RetryTask]:
        """Extract and remove tasks whose scheduled retry timestamp has arrived."""
        ready_tasks = []
        with self._lock:
            ready_ids = [tid for tid, task in self._tasks.items() if task.is_ready()]
            for tid in ready_ids:
                ready_tasks.append(self._tasks.pop(tid))

        if ready_tasks:
            logger.info(f"Retrieved {len(ready_tasks)} ready task(s) from Retry Queue.")
        return ready_tasks

    def peek_all(self) -> List[Dict[str, Any]]:
        """Return read-only snapshot of all pending retry tasks."""
        with self._lock:
            return [task.to_dict() for task in self._tasks.values()]

    def size(self) -> int:
        """Return current count of pending retry tasks."""
        with self._lock:
            return len(self._tasks)

    def clear(self) -> int:
        """Clear all tasks from the retry queue."""
        with self._lock:
            count = len(self._tasks)
            self._tasks.clear()
            return count

