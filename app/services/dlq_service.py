"""
Dead Letter Queue (DLQ) Service.
Stores permanently failed jobs that exhausted retries or experienced fatal non-retryable errors.
Supports diagnostics inspection, manual replay execution, and retention cleanup policies.
"""

import time
import uuid
import threading
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta

from app.utils.constants import DLQStatus
from app.utils.telemetry import metrics_collector

logger = logging.getLogger(__name__)

class DeadLetterQueue:
    """
    Thread-safe repository for dead-letter ingestion jobs.
    """

    def __init__(self, max_size: int = 5000, retention_days: int = 30) -> None:
        self.max_size = max_size
        self.retention_days = retention_days
        self._items: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def enqueue_dlq(
        self,
        job_id: str,
        target_object: str,
        query: str,
        error_message: str,
        error_type: Optional[str] = "Exception",
        retry_count: int = 0,
        payload: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Quarantine a permanently failed job into the Dead Letter Queue.
        """
        dlq_id = str(uuid.uuid4())
        item = {
            "dlq_id": dlq_id,
            "job_id": job_id,
            "target_object": target_object,
            "query": query,
            "error_type": error_type,
            "error_message": error_message,
            "retry_count": retry_count,
            "status": DLQStatus.PENDING_REVIEW.value,
            "payload": payload or {},
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "timestamp_epoch": time.time(),
            "replayed_at": None,
            "resolved_at": None
        }

        with self._lock:
            # Enforce max DLQ size policy (purge oldest if saturated)
            if len(self._items) >= self.max_size:
                oldest_id = min(self._items.keys(), key=lambda k: self._items[k]["timestamp_epoch"])
                self._items.pop(oldest_id, None)

            self._items[dlq_id] = item

        metrics_collector.set_dlq_size(len(self._items))
        logger.error(
            f"Job {job_id} quarantined in Dead Letter Queue (DLQ ID: {dlq_id}). "
            f"Reason: {error_message}"
        )
        return dlq_id

    def list_items(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Retrieve paginated list of quarantined DLQ jobs."""
        with self._lock:
            items = list(self._items.values())

        if status:
            items = [it for it in items if it.get("status") == status]

        # Order descending by failure timestamp
        sorted_items = sorted(items, key=lambda x: x["timestamp_epoch"], reverse=True)
        return sorted_items[offset: offset + limit]

    def get_item(self, dlq_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific DLQ item by DLQ ID."""
        with self._lock:
            return self._items.get(dlq_id)

    def replay_item(
        self,
        dlq_id: str,
        orchestrator: Any = None
    ) -> Dict[str, Any]:
        """
        Replay a quarantined job from DLQ.
        Marks item as REPLAYED and re-dispatches to orchestrator if provided.
        """
        with self._lock:
            item = self._items.get(dlq_id)
            if not item:
                raise ValueError(f"DLQ item '{dlq_id}' not found.")

            item["status"] = DLQStatus.REPLAYED.value
            item["replayed_at"] = datetime.now(timezone.utc).isoformat()

        logger.info(f"Replaying DLQ job {dlq_id} (Original Job: {item['job_id']})...")

        replayed_job_info = None
        if orchestrator is not None and hasattr(orchestrator, "submit_ingestion"):
            replayed_job_info = orchestrator.submit_ingestion(
                target_object=item["target_object"],
                query=item["query"]
            )

        return {
            "dlq_id": dlq_id,
            "status": DLQStatus.REPLAYED.value,
            "original_job_id": item["job_id"],
            "replayed_job": replayed_job_info,
            "message": f"DLQ item {dlq_id} marked as REPLAYED and submitted."
        }

    def replay_all(self, orchestrator: Any = None) -> List[Dict[str, Any]]:
        """Replay all pending jobs in the DLQ."""
        pending_ids = []
        with self._lock:
            pending_ids = [
                dlq_id for dlq_id, item in self._items.items()
                if item["status"] == DLQStatus.PENDING_REVIEW.value
            ]

        results = []
        for dlq_id in pending_ids:
            try:
                res = self.replay_item(dlq_id=dlq_id, orchestrator=orchestrator)
                results.append(res)
            except Exception as exc:
                logger.error(f"Failed to replay DLQ item {dlq_id}: {str(exc)}")
        return results

    def cleanup_dlq(self, retention_days: Optional[int] = None) -> int:
        """Purge DLQ items older than retention threshold."""
        days = retention_days if retention_days is not None else self.retention_days
        cutoff_epoch = time.time() - (days * 86400)
        purged = 0

        with self._lock:
            expired_ids = [
                dlq_id for dlq_id, item in self._items.items()
                if item["timestamp_epoch"] <= cutoff_epoch
            ]
            for dlq_id in expired_ids:
                self._items.pop(dlq_id, None)
                purged += 1

        metrics_collector.set_dlq_size(len(self._items))
        logger.info(f"Purged {purged} expired DLQ items older than {days} days.")
        return purged

    def clear(self) -> int:
        """Clear all items in the DLQ."""
        with self._lock:
            count = len(self._items)
            self._items.clear()
        metrics_collector.set_dlq_size(0)
        return count

    def size(self) -> int:
        """Return total count of quarantined items in DLQ."""
        with self._lock:
            return len(self._items)
