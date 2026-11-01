"""
Adaptive Asynchronous Status Polling Service.
Implements Decorrelated Exponential Jitter backoff algorithm to poll
Salesforce Bulk API 2.0 query jobs until reaching a terminal state.
"""

import time
import random
import logging
from typing import Dict, Any, Optional

import requests

from app.integrations.salesforce_client import SalesforceClient
from app.utils.constants import SalesforceJobState
from app.utils.exceptions import (
    PollingException,
    BulkQueryTimeoutError,
    SalesforceAPIError
)
from app.utils.telemetry import metrics_collector

logger = logging.getLogger(__name__)

class PollingService:
    """
    Asynchronous status poller for Salesforce Bulk API 2.0 query execution.
    Features decorrelated jitter backoff, transient error resilience,
    configurable timeouts, and explicit state detection.
    """

    def __init__(self, salesforce_client: Optional[SalesforceClient] = None) -> None:
        self.salesforce_client = salesforce_client or SalesforceClient()

    @staticmethod
    def is_job_complete(state: str) -> bool:
        """Check if job has reached JobComplete state."""
        return state == SalesforceJobState.JOB_COMPLETE.value

    @staticmethod
    def is_job_failed(state: str) -> bool:
        """Check if job has reached Failed state."""
        return state == SalesforceJobState.FAILED.value

    @staticmethod
    def is_job_aborted(state: str) -> bool:
        """Check if job has reached Aborted state."""
        return state == SalesforceJobState.ABORTED.value

    @staticmethod
    def is_job_in_progress(state: str) -> bool:
        """Check if job is actively InProgress."""
        return state == SalesforceJobState.IN_PROGRESS.value

    @staticmethod
    def is_job_upload_complete(state: str) -> bool:
        """Check if job is in initial UploadComplete state."""
        return state == SalesforceJobState.UPLOAD_COMPLETE.value

    def detect_state(self, job_status: Dict[str, Any]) -> str:
        """Extract and validate the state string from job metadata."""
        state = job_status.get("state", "Unknown")
        return state

    def poll_until_complete(
        self,
        sf_job_id: str,
        base_interval: float = 2.0,
        max_interval: float = 30.0,
        max_attempts: int = 120,
        timeout_seconds: int = 900,
        max_transient_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Poll Bulk API 2.0 query job state using Decorrelated Jitter Backoff.
        
        Args:
            sf_job_id: Salesforce 750... job identifier.
            base_interval: Initial sleep interval in seconds (default: 2.0).
            max_interval: Maximum ceiling sleep interval in seconds (default: 30.0).
            max_attempts: Maximum polling loop iterations (default: 120).
            timeout_seconds: Absolute timeout in seconds (default: 900 / 15 minutes).
            max_transient_retries: Consecutive transient network/server retries allowed (default: 3).
            
        Returns:
            dict: Terminal job metadata when state == 'JobComplete'.
            
        Raises:
            PollingException: If job enters 'Failed' or 'Aborted' state, or polling times out.
            BulkQueryTimeoutError: If duration exceeds timeout_seconds.
        """
        start_time = time.time()
        current_sleep = base_interval
        attempts = 0
        consecutive_transient_errors = 0

        logger.info(
            f"Starting status polling for Bulk Job {sf_job_id} "
            f"(base={base_interval}s, max={max_interval}s, timeout={timeout_seconds}s)"
        )

        while attempts < max_attempts:
            attempts += 1
            elapsed_time = time.time() - start_time

            if elapsed_time > timeout_seconds:
                logger.error(f"Polling exceeded timeout of {timeout_seconds}s for job {sf_job_id}")
                raise BulkQueryTimeoutError(
                    message=f"Polling exceeded timeout threshold of {timeout_seconds}s for job {sf_job_id}",
                    details={"elapsed_seconds": elapsed_time, "attempts": attempts}
                )

            # Query real-time status from Salesforce with transient failure resilience
            poll_req_start = time.time()
            try:
                job_status = self.salesforce_client.get_job_status(sf_job_id)
                consecutive_transient_errors = 0
            except (SalesforceAPIError, requests.RequestException) as exc:
                consecutive_transient_errors += 1
                logger.warning(
                    f"[Attempt {attempts}] Transient error polling job {sf_job_id} "
                    f"({consecutive_transient_errors}/{max_transient_retries}): {str(exc)}"
                )
                if consecutive_transient_errors > max_transient_retries:
                    logger.error(f"Exceeded max transient retries ({max_transient_retries}) polling {sf_job_id}")
                    raise
                time.sleep(base_interval)
                continue

            poll_latency_ms = (time.time() - poll_req_start) * 1000.0
            metrics_collector.record_polling_latency(poll_latency_ms)

            state = self.detect_state(job_status)
            records_processed = job_status.get("numberRecordsProcessed", 0)

            logger.info(
                f"[Attempt {attempts}/{max_attempts}] Job {sf_job_id} State: {state} "
                f"| Processed: {records_processed} records | Elapsed: {elapsed_time:.1f}s"
            )

            # 1. Terminal State: SUCCESS (JobComplete)
            if self.is_job_complete(state):
                total_duration = round(time.time() - start_time, 2)
                job_status["total_polling_time_seconds"] = total_duration
                job_status["polling_attempts"] = attempts
                logger.info(
                    f"Bulk Job {sf_job_id} completed successfully in {total_duration}s "
                    f"after {attempts} attempts. Records processed: {records_processed}"
                )
                return job_status

            # 2. Terminal State: FAILED
            if self.is_job_failed(state):
                err_msg = job_status.get("errorMessage", "Unknown Salesforce internal failure")
                logger.error(f"Bulk Job {sf_job_id} failed on Salesforce: {err_msg}")
                raise PollingException(
                    message=f"Salesforce Bulk API job failed: {err_msg}",
                    status_code=502,
                    details=job_status
                )

            # 3. Terminal State: ABORTED
            if self.is_job_aborted(state):
                logger.warning(f"Bulk Job {sf_job_id} was aborted.")
                raise PollingException(
                    message=f"Salesforce Bulk API job was aborted: {sf_job_id}",
                    status_code=409,
                    details=job_status
                )

            # 4. In Progress / Upload Complete -> Decorrelated Jitter Calculation
            # Formula: sleep = min(max_interval, Uniform(base_interval, current_sleep * 3))
            next_sleep = min(
                max_interval,
                random.uniform(base_interval, current_sleep * 3)
            )
            current_sleep = next_sleep

            logger.debug(f"Sleeping for {current_sleep:.2f}s before next poll for job {sf_job_id}...")
            time.sleep(current_sleep)

        logger.error(f"Exhausted maximum polling attempts ({max_attempts}) for job {sf_job_id}")
        raise BulkQueryTimeoutError(
            message=f"Exhausted maximum polling attempts ({max_attempts}) for Bulk Job {sf_job_id}",
            details={"attempts": attempts, "last_state": state}
        )
