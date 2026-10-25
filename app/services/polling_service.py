"""
Adaptive Asynchronous Status Polling Service.
Implements Decorrelated Exponential Jitter backoff algorithm to poll
Salesforce Bulk API 2.0 query jobs until reaching a terminal state.
"""

import time
import random
import logging
from typing import Dict, Any, Optional

from app.integrations.salesforce_client import SalesforceClient
from app.utils.constants import SalesforceJobState
from app.utils.exceptions import PollingException
from app.utils.telemetry import metrics_collector

logger = logging.getLogger(__name__)

class PollingService:
    """
    Asynchronous status poller for Salesforce Bulk API 2.0 query execution.
    """

    def __init__(self, salesforce_client: Optional[SalesforceClient] = None) -> None:
        self.salesforce_client = salesforce_client or SalesforceClient()

    def poll_until_complete(
        self,
        sf_job_id: str,
        base_interval: float = 2.0,
        max_interval: float = 30.0,
        max_attempts: int = 120,
        timeout_seconds: int = 900
    ) -> Dict[str, Any]:
        """
        Poll Bulk API 2.0 query job state using Decorrelated Jitter Backoff.
        
        Args:
            sf_job_id: Salesforce 750... job identifier.
            base_interval: Initial sleep interval in seconds (default: 2.0).
            max_interval: Maximum ceiling sleep interval in seconds (default: 30.0).
            max_attempts: Maximum polling loop iterations (default: 120).
            timeout_seconds: Absolute timeout in seconds (default: 900 / 15 minutes).
            
        Returns:
            dict: Terminal job metadata when state == 'JobComplete'.
            
        Raises:
            PollingException: If job enters 'Failed' or 'Aborted' state, or polling times out.
        """
        start_time = time.time()
        current_sleep = base_interval
        attempts = 0

        logger.info(
            f"Starting status polling for Bulk Job {sf_job_id} "
            f"(base={base_interval}s, max={max_interval}s, timeout={timeout_seconds}s)"
        )

        while attempts < max_attempts:
            attempts += 1
            elapsed_time = time.time() - start_time

            if elapsed_time > timeout_seconds:
                raise PollingException(
                    message=f"Polling exceeded timeout threshold of {timeout_seconds}s for job {sf_job_id}",
                    status_code=504,
                    details={"elapsed_seconds": elapsed_time, "attempts": attempts}
                )

            # Query real-time status from Salesforce
            poll_req_start = time.time()
            job_status = self.salesforce_client.get_job_status(sf_job_id)
            poll_latency_ms = (time.time() - poll_req_start) * 1000.0
            metrics_collector.record_polling_latency(poll_latency_ms)

            state = job_status.get("state")
            records_processed = job_status.get("numberRecordsProcessed", 0)

            logger.info(
                f"[Attempt {attempts}/{max_attempts}] Job {sf_job_id} State: {state} "
                f"| Processed: {records_processed} records | Elapsed: {elapsed_time:.1f}s"
            )

            # 1. Terminal State: SUCCESS
            if state == SalesforceJobState.JOB_COMPLETE.value:
                job_status["total_polling_time_seconds"] = round(time.time() - start_time, 2)
                job_status["polling_attempts"] = attempts
                return job_status

            # 2. Terminal State: FAILED
            if state == SalesforceJobState.FAILED.value:
                err_msg = job_status.get("errorMessage", "Unknown Salesforce internal failure")
                logger.error(f"Bulk Job {sf_job_id} failed on Salesforce: {err_msg}")
                raise PollingException(
                    message=f"Salesforce Bulk API job failed: {err_msg}",
                    status_code=502,
                    details=job_status
                )

            # 3. Terminal State: ABORTED
            if state == SalesforceJobState.ABORTED.value:
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

            logger.debug(f"Sleeping for {current_sleep:.2f}s before next poll...")
            time.sleep(current_sleep)

        raise PollingException(
            message=f"Exhausted maximum polling attempts ({max_attempts}) for Bulk Job {sf_job_id}",
            status_code=504,
            details={"attempts": attempts, "last_state": state}
        )
