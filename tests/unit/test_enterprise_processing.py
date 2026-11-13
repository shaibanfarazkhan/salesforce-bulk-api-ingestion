"""
Unit Tests for Phase 7: Enterprise Processing, Resilience & Monitoring.
Covers Circuit Breaker, Retry Queue, Dead Letter Queue (DLQ), Worker Pool,
Scheduler, Orchestrator, Monitoring, and Structured Logging.
"""

import time
import logging
import requests
from unittest.mock import MagicMock, patch
import pytest

from app.utils.constants import CircuitState, DLQStatus, SchedulerStatus
from app.utils.exceptions import (
    CircuitBreakerOpenException,
    QueueFullException,
    InvalidSOQLError,
    SalesforceAPIError,
    BulkQueryTimeoutError
)
from app.utils.resilience import is_retryable_error, CircuitBreaker, compute_decorrelated_jitter
from app.services.retry_queue import RetryQueue
from app.services.dlq_service import DeadLetterQueue
from app.services.worker_pool import IngestionWorkerPool
from app.services.scheduler_service import IngestionScheduler
from app.services.orchestrator_service import BulkIngestionOrchestrator
from app.services.monitoring_service import MonitoringService
from app.middleware.logging import JSONStructuredFormatter
from app.utils.telemetry import metrics_collector

# ==============================================================================
# 1. Resilience & Error Classification Tests
# ==============================================================================
def test_is_retryable_error_classification():
    """Verify error classification between retryable and non-retryable."""
    assert is_retryable_error(requests.exceptions.ConnectionError("Connection dropped")) is True
    assert is_retryable_error(requests.exceptions.Timeout("Read timeout")) is True
    assert is_retryable_error(BulkQueryTimeoutError("SOQL timeout")) is True
    assert is_retryable_error(SalesforceAPIError("Gateway error", status_code=503)) is True

    # Non-retryable
    assert is_retryable_error(InvalidSOQLError("Syntax error")) is False
    assert is_retryable_error(ValueError("Invalid format")) is False

def test_circuit_breaker_state_transitions():
    """Verify Circuit Breaker CLOSED -> OPEN -> HALF-OPEN -> CLOSED lifecycle."""
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1, name="TestCircuit")
    assert cb.state == CircuitState.CLOSED
    assert cb.can_execute() is True

    # Record 1 failure -> still closed
    cb.record_failure(Exception("Transient 1"))
    assert cb.state == CircuitState.CLOSED

    # Record 2nd failure -> trips to OPEN
    cb.record_failure(Exception("Transient 2"))
    assert cb.state == CircuitState.OPEN
    assert cb.can_execute() is False

    # Blocked execution raises CircuitBreakerOpenException
    with pytest.raises(CircuitBreakerOpenException):
        cb.execute(lambda: "should not run")

    # Wait for recovery timeout -> transitions to HALF_OPEN
    time.sleep(0.12)
    assert cb.state == CircuitState.HALF_OPEN
    assert cb.can_execute() is True

    # Successful execution resets to CLOSED
    res = cb.execute(lambda: "success!")
    assert res == "success!"
    assert cb.state == CircuitState.CLOSED

# ==============================================================================
# 2. Retry Queue Tests
# ==============================================================================
def test_retry_queue_enqueue_and_readiness():
    """Verify retry queue jittered backoff, peek, readiness check, and clearing."""
    rq = RetryQueue(base_delay=0.05, max_delay=0.2, max_retries=3)
    task = rq.enqueue(
        job_id="test-job-001",
        target_object="Account",
        query="SELECT Id FROM Account",
        error=SalesforceAPIError("Temporary 503", status_code=503),
        attempt=1
    )
    assert task is not None
    assert task.attempt == 1
    assert task.max_retries == 3
    assert rq.size() == 1

    # Not ready immediately
    assert len(rq.get_ready_tasks()) == 0

    # Wait for jittered delay
    time.sleep(0.25)
    ready = rq.get_ready_tasks()
    assert len(ready) == 1
    assert ready[0].job_id == "test-job-001"
    assert rq.size() == 0

    # Exceeding max retries returns None
    excess = rq.enqueue("test-job-002", "Account", "SELECT Id FROM Account", Exception("Err"), attempt=4)
    assert excess is None

# ==============================================================================
# 3. Dead Letter Queue (DLQ) Tests
# ==============================================================================
def test_dead_letter_queue_lifecycle():
    """Verify DLQ quarantine, inspection, manual replay, and retention cleanup."""
    dlq = DeadLetterQueue(max_size=10, retention_days=1)
    dlq_id = dlq.enqueue_dlq(
        job_id="job-fatal-001",
        target_object="Contact",
        query="SELECT Id FROM Contact",
        error_message="Non-recoverable syntax error",
        error_type="InvalidSOQLError",
        retry_count=3
    )
    assert dlq_id is not None
    assert dlq.size() == 1

    items = dlq.list_items()
    assert len(items) == 1
    assert items[0]["dlq_id"] == dlq_id
    assert items[0]["status"] == DLQStatus.PENDING_REVIEW.value

    # Replay DLQ item
    replay_res = dlq.replay_item(dlq_id=dlq_id)
    assert replay_res["status"] == DLQStatus.REPLAYED.value
    assert dlq.get_item(dlq_id)["status"] == DLQStatus.REPLAYED.value

    # Retention cleanup
    dlq._items[dlq_id]["timestamp_epoch"] = time.time() - 10.0
    purged = dlq.cleanup_dlq(retention_days=0)  # purge items older than 0 days
    assert purged == 1
    assert dlq.size() == 0

# ==============================================================================
# 4. Worker Pool Tests
# ==============================================================================
def test_worker_pool_concurrency_and_shutdown():
    """Verify worker pool task execution across threads and graceful shutdown."""
    pool = IngestionWorkerPool(max_workers=2, max_queue_size=5)
    pool.start()
    assert pool.is_running() is True

    results = []
    def sample_worker_task(val: int):
        time.sleep(0.02)
        results.append(val * 2)

    task1 = pool.submit(sample_worker_task, 5)
    task2 = pool.submit(sample_worker_task, 10)
    assert task1 is not None
    assert task2 is not None

    time.sleep(0.1)
    assert 10 in results
    assert 20 in results

    stats = pool.get_stats()
    assert stats["max_workers"] == 2
    assert stats["completed_tasks"] == 2

    pool.shutdown(wait=True, timeout=1.0)
    assert pool.is_running() is False

# ==============================================================================
# 5. Background Scheduler Tests
# ==============================================================================
def test_scheduler_lifecycle_and_manual_trigger():
    """Verify scheduler job registration, status, manual trigger, pause/resume."""
    executed = []
    def sample_executor(obj, query):
        executed.append(obj)
        return {"status": "SYNCED", "object": obj}

    scheduler = IngestionScheduler(job_executor=sample_executor)
    job = scheduler.add_job(
        name="daily-account-sync",
        target_object="Account",
        query="SELECT Id FROM Account",
        interval_seconds=3600
    )
    assert job.name == "daily-account-sync"
    assert scheduler.get_status()["job_count"] == 1

    # Manual trigger immediately runs executor without waiting
    res = scheduler.trigger_job_now("daily-account-sync")
    assert res["status"] == "SUCCESS"
    assert "Account" in executed
    assert job.run_count == 1

    scheduler.start()
    assert scheduler.get_status()["is_running"] is True

    scheduler.pause()
    assert scheduler.get_status()["is_paused"] is True

    scheduler.resume()
    assert scheduler.get_status()["is_paused"] is False

    scheduler.stop(wait=True, timeout=1.0)
    assert scheduler.get_status()["is_running"] is False

# ==============================================================================
# 6. Bulk Ingestion Orchestrator Tests
# ==============================================================================
def test_orchestrator_pipeline_and_retry_routing(app):
    """Verify orchestrator routes transient failures to RetryQueue."""
    with app.app_context():
        mock_bulk_service = MagicMock()
        mock_bulk_service.create_job.return_value = {
            "job_id": "orch-job-001",
            "sf_job_id": "750300000001ORCH01"
        }
        mock_job = MagicMock()
        mock_job.id = "orch-job-001"
        mock_job.sf_job_id = "750300000001ORCH01"
        mock_job.target_object = "Account"
        mock_bulk_service._find_job.return_value = mock_job
        mock_bulk_service.polling_service.poll_until_complete.side_effect = SalesforceAPIError("Transient 503", status_code=503)

        retry_queue = RetryQueue(base_delay=0.01, max_delay=0.05, max_retries=3)
        dlq = DeadLetterQueue()
        mock_repo = MagicMock()

        orchestrator = BulkIngestionOrchestrator(
            bulk_service=mock_bulk_service,
            retry_queue=retry_queue,
            dlq=dlq,
            bulk_repo=mock_repo
        )

        res = orchestrator.execute_pipeline(
            job_identifier="orch-job-001",
            target_object="Account",
            query="SELECT Id FROM Account",
            attempt=1
        )
        assert res["status"] == "RETRY_SCHEDULED"
        assert res["next_attempt"] == 2
        assert retry_queue.size() == 1
        assert dlq.size() == 0

# ==============================================================================
# 7. Monitoring & Structured Logging Tests
# ==============================================================================
def test_monitoring_dashboard_and_structured_logs():
    """Verify monitoring service dashboard output, probes, and structured JSON logs."""
    monitoring = MonitoringService()
    dashboard = monitoring.get_dashboard_summary()
    assert "system" in dashboard
    assert "job_metrics" in dashboard
    assert "throughput" in dashboard

    readiness = monitoring.get_readiness_probe()
    assert "ready" in readiness

    liveness = monitoring.get_liveness_probe()
    assert liveness["status"] == "ALIVE"

    prom_metrics = monitoring.get_prometheus_metrics()
    assert "sf_ingest_uptime_seconds" in prom_metrics
    assert "sf_ingest_total_jobs_total" in prom_metrics

    # Test JSONStructuredFormatter
    formatter = JSONStructuredFormatter()
    log_record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=42,
        msg="Test enterprise log message",
        args=(),
        exc_info=None
    )
    formatted_json = formatter.format(log_record)
    assert '"message": "Test enterprise log message"' in formatted_json
    assert '"level": "INFO"' in formatted_json
    assert '"timestamp":' in formatted_json
