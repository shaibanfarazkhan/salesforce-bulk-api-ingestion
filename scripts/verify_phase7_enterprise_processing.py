"""
Phase 7 Enterprise Processing, Monitoring & Resilience Verification Script.
Validates the complete enterprise orchestration, resilience, and monitoring layer:
1. Configuration Loading (Environment-driven concurrency, retries, queues, scheduler)
2. Worker Lifecycle & Bounded Parallel Processing
3. Safe Concurrency & Queue Execution
4. Retry Queue (Decorrelated Jitter Backoff & Persistence)
5. Dead Letter Queue (DLQ Diagnostics, Replay & Retention Cleanup)
6. Error Classification & Circuit Breaker State Machine
7. Bulk Ingestion Orchestrator Workflow & State Transitions
8. Workflow Recovery for Interrupted Jobs
9. Background Job Scheduler Startup, Health & Manual Trigger
10. Prometheus-Compatible Metrics Collection
11. Health Probes (Readiness & Liveness Endpoints)
12. Structured JSON Logging (Correlation ID, Request ID, Timing & Error Context)
13. Graceful Shutdown of Worker Pool and Scheduler
"""

import sys
import time
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure root directory is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import create_app
from app.config import get_config
from app.utils.constants import CircuitState, DLQStatus, SchedulerStatus, IngestionStatus
from app.utils.exceptions import (
    CircuitBreakerOpenException,
    InvalidSOQLError,
    SalesforceAPIError,
    BulkQueryTimeoutError
)
from app.utils.resilience import is_retryable_error, CircuitBreaker, compute_decorrelated_jitter
from app.services.worker_pool import IngestionWorkerPool
from app.services.retry_queue import RetryQueue
from app.services.dlq_service import DeadLetterQueue
from app.services.scheduler_service import IngestionScheduler
from app.services.orchestrator_service import BulkIngestionOrchestrator
from app.services.monitoring_service import MonitoringService
from app.middleware.logging import JSONStructuredFormatter
from app.utils.telemetry import metrics_collector
from app.database.database import db
from app.database.models import IngestionJob, AuditLog

def run_phase7_verification() -> bool:
    print("=" * 75)
    print("SALESFORCE BULK API 2.0 INGESTION SYSTEM - PHASE 7 VERIFICATION")
    print("=" * 75)

    checks = []

    # 1. Configuration Loading
    try:
        cfg = get_config("development")
        has_workers = hasattr(cfg, "MAX_WORKERS") and cfg.MAX_WORKERS >= 1
        has_retries = hasattr(cfg, "MAX_RETRIES") and cfg.MAX_RETRIES >= 1
        has_jitter = hasattr(cfg, "RETRY_BASE_DELAY") and cfg.RETRY_BASE_DELAY > 0
        has_dlq = hasattr(cfg, "DLQ_MAX_SIZE") and cfg.DLQ_MAX_SIZE > 0
        has_sched = hasattr(cfg, "SCHEDULER_INTERVAL_SECONDS") and cfg.SCHEDULER_INTERVAL_SECONDS > 0

        config_ok = has_workers and has_retries and has_jitter and has_dlq and has_sched
        config_detail = (
            f"Workers: {cfg.MAX_WORKERS} | Retries: {cfg.MAX_RETRIES} | "
            f"DLQ Cap: {cfg.DLQ_MAX_SIZE} | Sched Interval: {cfg.SCHEDULER_INTERVAL_SECONDS}s"
        )
    except Exception as exc:
        config_ok = False
        config_detail = f"Config error: {str(exc)}"
    checks.append(("Configuration Loading", config_ok, config_detail))

    # 2. Worker Lifecycle
    worker_pool = None
    try:
        worker_pool = IngestionWorkerPool(max_workers=3, max_queue_size=20)
        assert worker_pool.is_running() is False
        worker_pool.start()
        is_running = worker_pool.is_running()
        stats = worker_pool.get_stats()

        lifecycle_ok = is_running and (stats["max_workers"] == 3) and (stats["active_workers"] == 0)
        lifecycle_detail = f"Started pool: {stats['max_workers']} workers (Active: {stats['active_workers']}, Idle: {stats['idle_workers']})"
    except Exception as exc:
        lifecycle_ok = False
        lifecycle_detail = f"Worker lifecycle error: {str(exc)}"
    checks.append(("Worker Lifecycle", lifecycle_ok, lifecycle_detail))

    # 3. Parallel Processing & Queue Execution
    try:
        processed_items = []
        def parallel_task(val: int):
            time.sleep(0.01)
            processed_items.append(val)

        for i in range(6):
            worker_pool.submit(parallel_task, i)

        # Allow worker threads to consume tasks
        time.sleep(0.15)
        parallel_ok = len(processed_items) == 6 and set(processed_items) == set(range(6))
        parallel_detail = f"Processed {len(processed_items)} tasks concurrently across worker threads"
    except Exception as exc:
        parallel_ok = False
        parallel_detail = f"Parallel processing error: {str(exc)}"
    checks.append(("Parallel Processing", parallel_ok, parallel_detail))

    # 4. Retry Queue with Decorrelated Jitter
    try:
        retry_q = RetryQueue(base_delay=0.02, max_delay=0.1, max_retries=3)
        task = retry_q.enqueue(
            job_id="job-retry-01",
            target_object="Account",
            query="SELECT Id FROM Account",
            error=SalesforceAPIError("Transient 503 Gateway Error", status_code=503),
            attempt=1
        )
        has_task = task is not None and task.delay_seconds >= 0.02
        initial_ready = len(retry_q.get_ready_tasks()) == 0

        # Wait for jitter delay
        time.sleep(0.15)
        ready_now = len(retry_q.get_ready_tasks()) == 1

        retry_ok = has_task and initial_ready and ready_now and retry_q.size() == 0
        retry_detail = f"Jittered backoff ({task.delay_seconds}s) scheduled -> Successfully retrieved upon readiness"
    except Exception as exc:
        retry_ok = False
        retry_detail = f"Retry queue error: {str(exc)}"
    checks.append(("Retry Queue (Decorrelated Jitter)", retry_ok, retry_detail))

    # 5. Dead Letter Queue (DLQ)
    try:
        dlq = DeadLetterQueue(max_size=50, retention_days=1)
        dlq_id = dlq.enqueue_dlq(
            job_id="job-fatal-01",
            target_object="Opportunity",
            query="SELECT InvalidField FROM Opportunity",
            error_message="Fatal query syntax error: InvalidField",
            error_type="InvalidSOQLError",
            retry_count=3
        )
        item = dlq.get_item(dlq_id)
        assert item is not None and item["status"] == DLQStatus.PENDING_REVIEW.value

        # Replay item
        replay_res = dlq.replay_item(dlq_id)
        replayed_ok = replay_res["status"] == DLQStatus.REPLAYED.value

        # Retention cleanup
        dlq._items[dlq_id]["timestamp_epoch"] = time.time() - 10.0
        purged = dlq.cleanup_dlq(retention_days=0)

        dlq_ok = dlq_id is not None and replayed_ok and (purged == 1) and (dlq.size() == 0)
        dlq_detail = f"Quarantined fatal job -> Replayed successfully -> Purged via retention policy"
    except Exception as exc:
        dlq_ok = False
        dlq_detail = f"DLQ error: {str(exc)}"
    checks.append(("Dead Letter Queue (DLQ)", dlq_ok, dlq_detail))

    # 6. Error Classification & Circuit Breaker
    try:
        # 1. Error classification
        retryable = is_retryable_error(SalesforceAPIError("Gateway Timeout", status_code=504))
        non_retryable = not is_retryable_error(InvalidSOQLError("Bad syntax"))
        classification_ok = retryable and non_retryable

        # 2. Circuit breaker state machine
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.08, name="SF-Verify-Breaker")
        assert cb.state == CircuitState.CLOSED

        cb.record_failure(Exception("Fail 1"))
        cb.record_failure(Exception("Fail 2"))
        assert cb.state == CircuitState.OPEN

        tripped_ok = False
        try:
            cb.execute(lambda: "blocked")
        except CircuitBreakerOpenException:
            tripped_ok = True

        time.sleep(0.1)
        assert cb.state == CircuitState.HALF_OPEN

        cb.record_success()
        assert cb.state == CircuitState.CLOSED

        cb_ok = classification_ok and tripped_ok and (cb.state == CircuitState.CLOSED)
        cb_detail = "Retryable classified accurately; Circuit tripped OPEN on 2 failures -> Reset to CLOSED on success"
    except Exception as exc:
        cb_ok = False
        cb_detail = f"Circuit breaker error: {str(exc)}"
    checks.append(("Error Classification & Circuit Breaker", cb_ok, cb_detail))

    # 7. Orchestrator Workflow & State Transitions
    try:
        app = create_app("testing")
        with app.app_context():
            db.create_all()
            mock_sf_client = MagicMock()
            mock_sf_client.api_version = "v62.0"
            mock_sf_client.instance_url = "https://mock.salesforce.com"
            mock_sf_client.create_query_job.return_value = {
                "id": "750300000001ORCH99",
                "operation": "query",
                "object": "Account",
                "state": "UploadComplete"
            }
            mock_sf_client.get_job_status.return_value = {
                "id": "750300000001ORCH99",
                "state": "JobComplete",
                "numberRecordsProcessed": 1
            }

            resp_mock = MagicMock()
            resp_mock.iter_lines.return_value = [
                "Id,Name,SystemModstamp",
                "001300000001ORCHAC,Orchestrator Corp,2026-11-01T09:00:00Z"
            ]
            mock_sf_client.get_job_results_stream.return_value = (resp_mock, None)

            orchestrator = BulkIngestionOrchestrator(
                worker_pool=worker_pool,
                retry_queue=RetryQueue(),
                dlq=DeadLetterQueue()
            )
            orchestrator.bulk_service.salesforce_client = mock_sf_client
            orchestrator.bulk_service.polling_service.salesforce_client = mock_sf_client
            orchestrator.bulk_service.stream_service.salesforce_client = mock_sf_client

            with patch("time.sleep", return_value=None):
                result = orchestrator.submit_ingestion(
                    target_object="Account",
                    query="SELECT Id, Name, SystemModstamp FROM Account",
                    async_exec=False
                )

            orch_ok = (
                result["status"] == "COMPLETED" and
                result["records_ingested"] == 1
            )
            orch_detail = f"Lifecycle: SUBMITTED -> IN_PROGRESS -> STREAMING -> COMPLETED (Ingested: {result['records_ingested']})"
    except Exception as exc:
        orch_ok = False
        orch_detail = f"Orchestrator error: {str(exc)}"
    checks.append(("Orchestrator Workflow", orch_ok, orch_detail))

    # 8. Workflow Recovery
    try:
        with app.app_context():
            # Inject an interrupted job into DB
            interrupted_job = IngestionJob(
                sf_job_id="750300000001RECOV01",
                target_object="Account",
                operation="query",
                soql_query="SELECT Id, Name FROM Account",
                status=IngestionStatus.IN_PROGRESS.value,
                records_processed=0,
                records_ingested=0,
                records_failed=0
            )
            db.session.add(interrupted_job)
            db.session.commit()

            # Mock SF as JobComplete
            mock_sf_client.get_job_status.return_value = {
                "id": "750300000001RECOV01",
                "state": "JobComplete",
                "numberRecordsProcessed": 1
            }

            recovered_list = orchestrator.recover_interrupted_jobs()
            recovered_ok = any(r.get("job_id") == str(interrupted_job.id) for r in recovered_list)
            recovery_detail = f"Scanned DB and recovered {len(recovered_list)} interrupted job(s)"
    except Exception as exc:
        recovered_ok = False
        recovery_detail = f"Recovery error: {str(exc)}"
    checks.append(("Workflow Recovery", recovered_ok, recovery_detail))

    # 9. Scheduler Startup & Manual Trigger
    scheduler = None
    try:
        exec_counts = []
        def dummy_job_exec(entity, q):
            exec_counts.append(entity)
            return {"status": "SUCCESS", "entity": entity}

        scheduler = IngestionScheduler(job_executor=dummy_job_exec)
        scheduler.add_job("account-sync", "Account", "SELECT Id FROM Account", interval_seconds=3600)
        scheduler.start()
        sched_started = scheduler.get_status()["is_running"]

        # Manual trigger
        trigger_res = scheduler.trigger_job_now("account-sync")
        manual_ok = trigger_res["status"] == "SUCCESS" and "Account" in exec_counts

        # Pause & Resume
        scheduler.pause()
        paused_ok = scheduler.get_status()["is_paused"]
        scheduler.resume()
        resumed_ok = not scheduler.get_status()["is_paused"]

        sched_ok = sched_started and manual_ok and paused_ok and resumed_ok
        sched_detail = "Daemon thread started -> Manual trigger executed -> Pause/Resume verified"
    except Exception as exc:
        sched_ok = False
        sched_detail = f"Scheduler error: {str(exc)}"
    checks.append(("Scheduler Startup & Manual Trigger", sched_ok, sched_detail))

    # 10. Metrics Collection (Prometheus Exposition)
    try:
        metrics_collector.record_job_started()
        metrics_collector.record_job_completed()
        metrics_collector.record_retry()
        metrics_collector.set_active_workers(2)
        metrics_collector.set_queue_size(3)

        prom_metrics = metrics_collector.export_prometheus_metrics()
        prom_ok = (
            "sf_ingest_total_jobs_total" in prom_metrics and
            "sf_ingest_successful_jobs_total" in prom_metrics and
            "sf_ingest_retry_count_total" in prom_metrics and
            "sf_ingest_active_workers 2" in prom_metrics and
            "sf_ingest_queue_size 3" in prom_metrics
        )
        prom_detail = "Prometheus exposition format emitted: total_jobs, retries, active_workers, queue_size"
    except Exception as exc:
        prom_ok = False
        prom_detail = f"Metrics error: {str(exc)}"
    checks.append(("Metrics Collection (Prometheus)", prom_ok, prom_detail))

    # 11. Health Endpoints (Readiness & Liveness)
    try:
        monitoring = MonitoringService(worker_pool=worker_pool, scheduler=scheduler)
        readiness = monitoring.get_readiness_probe()
        liveness = monitoring.get_liveness_probe()

        health_ok = (
            readiness["status"] in ("READY", "NOT_READY") and
            "database" in readiness["checks"] and
            liveness["status"] == "ALIVE" and
            liveness["uptime_seconds"] > 0
        )
        health_detail = f"Readiness: {readiness['status']} | Liveness: {liveness['status']} (Uptime: {liveness['uptime_seconds']}s)"
    except Exception as exc:
        health_ok = False
        health_detail = f"Health probe error: {str(exc)}"
    checks.append(("Health Endpoints (Ready & Live)", health_ok, health_detail))

    # 12. Structured JSON Logging
    try:
        formatter = JSONStructuredFormatter()
        record = logging.LogRecord(
            name="test.orchestrator",
            level=logging.INFO,
            pathname="orchestrator.py",
            lineno=100,
            msg="Bulk API pipeline stage complete",
            args=(),
            exc_info=None
        )
        record.correlation_id = "CORR-TEST-12345"
        record.request_id = "REQ-TEST-67890"
        record.duration_ms = 45.67

        formatted = formatter.format(record)
        log_ok = (
            '"correlation_id": "CORR-TEST-12345"' in formatted and
            '"request_id": "REQ-TEST-67890"' in formatted and
            '"duration_ms": 45.67' in formatted and
            '"level": "INFO"' in formatted and
            '"timestamp":' in formatted
        )
        log_detail = "Formatted structured JSON: correlation_id, request_id, duration_ms, and ISO timestamp"
    except Exception as exc:
        log_ok = False
        log_detail = f"Logging format error: {str(exc)}"
    checks.append(("Structured JSON Logging", log_ok, log_detail))

    # 13. Graceful Shutdown
    try:
        if worker_pool:
            worker_pool.shutdown(wait=True, timeout=2.0)
            wp_stopped = not worker_pool.is_running()
        else:
            wp_stopped = True

        if scheduler:
            scheduler.stop(wait=True, timeout=2.0)
            sched_stopped = not scheduler.get_status()["is_running"]
        else:
            sched_stopped = True

        shutdown_ok = wp_stopped and sched_stopped
        shutdown_detail = f"Worker Pool stopped (running={not wp_stopped}) | Scheduler stopped (running={not sched_stopped})"
    except Exception as exc:
        shutdown_ok = False
        shutdown_detail = f"Shutdown error: {str(exc)}"
    checks.append(("Graceful Shutdown", shutdown_ok, shutdown_detail))

    # Print summary table
    all_passed = True
    for name, status, detail in checks:
        status_str = "PASS" if status else "FAIL"
        if not status:
            all_passed = False
        print(f"[{status_str}] {name:<38} : {detail}")

    print("=" * 75)
    if all_passed:
        print("PHASE 7 ENTERPRISE PROCESSING & RESILIENCE VERIFIED")
        print("=" * 75)
        return True
    else:
        print("PHASE 7 ENTERPRISE PROCESSING & RESILIENCE FAILED")
        print("=" * 75)
        return False

if __name__ == "__main__":
    if not run_phase7_verification():
        sys.exit(1)
    sys.exit(0)

