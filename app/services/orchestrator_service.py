"""
Bulk Ingestion Orchestrator Service.
Master enterprise coordinator managing end-to-end ingestion workflow lifecycle,
state transitions (SUBMITTED -> IN_PROGRESS -> STREAMING -> COMPLETED),
workflow recovery, resilience queues (Retry & DLQ), parallel execution, and circuit breaking.
"""

import time
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from app.integrations.salesforce_client import SalesforceClient
from app.repositories.bulk_repository import BulkRepository
from app.services.bulk_service import BulkService
from app.services.polling_service import PollingService
from app.services.csv_stream_service import CSVStreamService
from app.services.validation_service import ValidationService
from app.services.retry_queue import RetryQueue
from app.services.dlq_service import DeadLetterQueue
from app.services.worker_pool import IngestionWorkerPool
from app.utils.resilience import CircuitBreaker, is_retryable_error
from app.database.models import IngestionJob, AuditLog
from app.utils.constants import IngestionStatus, SalesforceJobState
from app.utils.exceptions import (
    BulkJobException,
    DeadLetterException,
    CircuitBreakerOpenException,
    ResourceNotFoundError
)
from app.utils.telemetry import metrics_collector

logger = logging.getLogger(__name__)

class BulkIngestionOrchestrator:
    """
    Enterprise-grade pipeline orchestrator providing lifecycle management,
    state recovery, circuit breaking, and resilience queuing.
    """

    def __init__(
        self,
        bulk_service: Optional[BulkService] = None,
        worker_pool: Optional[IngestionWorkerPool] = None,
        retry_queue: Optional[RetryQueue] = None,
        dlq: Optional[DeadLetterQueue] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        bulk_repo: Optional[BulkRepository] = None
    ) -> None:
        self.bulk_service = bulk_service or BulkService()
        self.worker_pool = worker_pool or IngestionWorkerPool(max_workers=4)
        self.retry_queue = retry_queue or RetryQueue()
        self.dlq = dlq or DeadLetterQueue()
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.bulk_repo = bulk_repo or self.bulk_service.bulk_repo

    def submit_ingestion(
        self,
        target_object: str,
        query: str,
        async_exec: bool = False
    ) -> Dict[str, Any]:
        """
        Submit a new bulk ingestion request.
        If async_exec=True, queues job in worker pool and returns immediately.
        """
        metrics_collector.record_job_started()

        # Step 1: Create local ledger job and Salesforce Query Job
        created_info = self.circuit_breaker.execute(
            self.bulk_service.create_job,
            target_object=target_object,
            query=query
        )
        job_id = created_info["job_id"]
        sf_job_id = created_info["sf_job_id"]

        logger.info(f"Orchestrator registered job {job_id} (SF ID: {sf_job_id}) for entity '{target_object}'")

        if async_exec:
            task_id = self.worker_pool.submit(
                self.execute_pipeline,
                job_identifier=job_id,
                target_object=target_object,
                query=query,
                attempt=1
            )
            return {
                "job_id": job_id,
                "sf_job_id": sf_job_id,
                "target_object": target_object,
                "status": IngestionStatus.SUBMITTED.value,
                "async_task_id": task_id,
                "message": "Job enqueued for asynchronous worker execution."
            }

        # Synchronous execution
        return self.execute_pipeline(
            job_identifier=job_id,
            target_object=target_object,
            query=query,
            attempt=1
        )

    def execute_pipeline(
        self,
        job_identifier: str,
        target_object: str,
        query: str,
        attempt: int = 1
    ) -> Dict[str, Any]:
        """
        Execute end-to-end pipeline stages:
        Polling -> Streaming -> Validation -> Upsert -> State Transitions.
        Catches errors and routes to RetryQueue or DeadLetterQueue.
        """
        job = self.bulk_service._find_job(job_identifier)
        start_time = time.time()

        try:
            # Stage 1: Transition to IN_PROGRESS
            job.status = IngestionStatus.IN_PROGRESS.value
            self.bulk_repo.update_job(job)
            self._log_audit(job.id, "STATUS_TRANSITION", target_object, 0, f"State transitioned to {job.status}")

            # Stage 2: Poll Salesforce Job Status protected by Circuit Breaker
            polled_meta = self.circuit_breaker.execute(
                self.bulk_service.polling_service.poll_until_complete,
                sf_job_id=job.sf_job_id
            )

            # Stage 3: Transition to STREAMING
            job.status = IngestionStatus.STREAMING.value
            self.bulk_repo.update_job(job)
            self._log_audit(job.id, "STATUS_TRANSITION", target_object, 0, f"State transitioned to {job.status}")

            # Stage 4: Stream and ingest results
            summary = self.bulk_service.start_ingestion(job_identifier=str(job.id))

            # Record success
            self.circuit_breaker.record_success()
            metrics_collector.record_job_completed()
            logger.info(f"Orchestrator successfully completed job {job.id} for {target_object}")
            return summary

        except Exception as exc:
            duration_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Pipeline error on job {job.id} (Attempt {attempt}): {str(exc)}", exc_info=True)

            retryable = is_retryable_error(exc)

            if retryable and attempt < self.retry_queue.max_retries:
                # Schedule in Retry Queue
                retry_task = self.retry_queue.enqueue(
                    job_id=str(job.id),
                    target_object=target_object,
                    query=query,
                    error=exc,
                    attempt=attempt + 1,
                    payload={"sf_job_id": job.sf_job_id}
                )
                self._log_audit(
                    job.id,
                    "JOB_RETRY_SCHEDULED",
                    target_object,
                    0,
                    f"Retry #{attempt + 1} scheduled in {retry_task.delay_seconds if retry_task else 0}s: {str(exc)}"
                )
                return {
                    "job_id": str(job.id),
                    "sf_job_id": job.sf_job_id,
                    "status": "RETRY_SCHEDULED",
                    "attempt": attempt,
                    "next_attempt": attempt + 1,
                    "error": str(exc)
                }
            else:
                # Non-retryable or retries exhausted -> Move to DLQ
                metrics_collector.record_job_failed()
                job.status = IngestionStatus.FAILED.value
                job.error_message = str(exc)
                job.completed_at = datetime.now(timezone.utc)
                job.execution_time_ms = duration_ms
                self.bulk_repo.update_job(job)

                dlq_id = self.dlq.enqueue_dlq(
                    job_id=str(job.id),
                    target_object=target_object,
                    query=query,
                    error_message=str(exc),
                    error_type=exc.__class__.__name__,
                    retry_count=attempt
                )
                self._log_audit(
                    job.id,
                    "JOB_SENT_TO_DLQ",
                    target_object,
                    0,
                    f"Job quarantined to Dead Letter Queue (DLQ ID: {dlq_id}): {str(exc)}"
                )
                raise

    def recover_interrupted_jobs(self) -> List[Dict[str, Any]]:
        """
        Recover incomplete or interrupted jobs (e.g. following process reboot).
        Inspects jobs in SUBMITTED, IN_PROGRESS, or STREAMING states and attempts recovery.
        """
        logger.info("Scanning for interrupted Bulk API jobs requiring recovery...")
        recovered_reports = []

        # Find incomplete jobs in database
        incomplete_statuses = [
            IngestionStatus.SUBMITTED.value,
            IngestionStatus.IN_PROGRESS.value,
            IngestionStatus.STREAMING.value
        ]

        try:
            incomplete_jobs = (
                self.bulk_repo.session.query(IngestionJob)
                .filter(IngestionJob.status.in_(incomplete_statuses))
                .all()
            )
        except Exception as exc:
            logger.error(f"Failed to query incomplete jobs for recovery: {str(exc)}")
            return []

        logger.info(f"Found {len(incomplete_jobs)} interrupted job(s) for workflow recovery.")

        for job in incomplete_jobs:
            if not job.sf_job_id:
                continue

            try:
                sf_status = self.bulk_service.salesforce_client.get_job_status(job.sf_job_id)
                sf_state = sf_status.get("state")

                if sf_state == SalesforceJobState.JOB_COMPLETE.value:
                    logger.info(f"Recovering completed SF job {job.sf_job_id} -> Executing ingestion")
                    res = self.bulk_service.start_ingestion(str(job.id))
                    recovered_reports.append({"job_id": str(job.id), "action": "INGESTED", "status": res["status"]})

                elif sf_state in (SalesforceJobState.IN_PROGRESS.value, SalesforceJobState.UPLOAD_COMPLETE.value):
                    logger.info(f"Resuming polling for active SF job {job.sf_job_id}")
                    # Re-queue in worker pool
                    self.worker_pool.submit(
                        self.execute_pipeline,
                        job_identifier=str(job.id),
                        target_object=job.target_object,
                        query=job.soql_query,
                        attempt=1
                    )
                    recovered_reports.append({"job_id": str(job.id), "action": "RE_QUEUED", "status": "IN_PROGRESS"})

                elif sf_state == SalesforceJobState.ABORTED.value:
                    job.status = IngestionStatus.ABORTED.value
                    self.bulk_repo.update_job(job)
                    recovered_reports.append({"job_id": str(job.id), "action": "MARKED_ABORTED"})

                elif sf_state == SalesforceJobState.FAILED.value:
                    job.status = IngestionStatus.FAILED.value
                    job.error_message = sf_status.get("errorMessage", "Salesforce execution failure")
                    self.bulk_repo.update_job(job)
                    self.dlq.enqueue_dlq(
                        job_id=str(job.id),
                        target_object=job.target_object,
                        query=job.soql_query,
                        error_message=job.error_message
                    )
                    recovered_reports.append({"job_id": str(job.id), "action": "SENT_TO_DLQ"})

            except Exception as exc:
                logger.error(f"Error during recovery of job {job.id}: {str(exc)}")
                recovered_reports.append({"job_id": str(job.id), "action": "ERROR", "error": str(exc)})

        return recovered_reports

    def _log_audit(
        self,
        job_id: Any,
        action: str,
        entity_type: str,
        record_count: int,
        message: str,
        payload: Optional[Dict[str, Any]] = None
    ) -> None:
        """Insert structured audit event."""
        try:
            self.bulk_repo.insert_logs([{
                "job_id": job_id,
                "action": action,
                "entity_type": entity_type,
                "record_count": record_count,
                "message": message,
                "payload": payload or {}
            }])
        except Exception as exc:
            logger.warning(f"Could not persist audit log for job {job_id}: {str(exc)}")

