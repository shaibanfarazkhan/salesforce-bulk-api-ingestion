"""
Phase 6 Salesforce Bulk API 2.0 Integration Verification Script.
Validates the complete Bulk API 2.0 Query lifecycle and features:
1. Authentication & OAuth integration with Bulk API client
2. Bulk Query Job Creation (POST /services/data/vXX.X/jobs/query) & SOQL validation
3. Real-time Job Status & Lifecycle State Detection (UploadComplete, InProgress, JobComplete, Failed, Aborted)
4. Automatic Polling with Decorrelated Jitter Backoff
5. Polling Transient Failure Resilience & Timeout Handling
6. Result Download with Sforce-Locator Pagination & Memory-Bounded CSV Streaming
7. CSV Parsing & Conversion to Structured JSON Objects
8. Job Management (Abort Query Job & Delete/Cleanup Job)
9. Enterprise Error Handling (HTTP 401 Auto-Refresh, HTTP 429 Rate Limits, Network Failures)
10. End-to-End Service Layer Orchestration (BulkService + Audit Ledger)
"""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
import requests

# Ensure root directory is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import create_app
from app.config import get_config
from app.integrations.auth import SalesforceOAuth
from app.integrations.salesforce_client import SalesforceClient
from app.services.polling_service import PollingService
from app.services.csv_stream_service import CSVStreamService
from app.services.bulk_service import BulkService
from app.repositories.bulk_repository import BulkRepository
from app.database.database import db
from app.database.models import IngestionJob, AuditLog
from app.utils.constants import SalesforceJobState, IngestionStatus
from app.utils.exceptions import (
    BulkJobException,
    InvalidSOQLError,
    PollingException,
    BulkQueryTimeoutError,
    DownloadException,
    RateLimitExceededError,
    SalesforceAPIError
)

def run_phase6_verification() -> bool:
    print("=" * 75)
    print("SALESFORCE BULK API 2.0 INGESTION SYSTEM - PHASE 6 VERIFICATION")
    print("=" * 75)

    checks = []

    # 1. Authentication & OAuth Integration with Bulk Client
    try:
        oauth = MagicMock(spec=SalesforceOAuth)
        oauth.get_instance_url.return_value = "https://enterprise-test.my.salesforce.com"
        oauth.get_authorization_header.return_value = {"Authorization": "Bearer 00DTESTBULKTOKEN"}
        oauth.session = requests.Session()

        client = SalesforceClient(oauth_manager=oauth, api_version="v62.0", timeout_seconds=30)
        headers = client.get_headers()
        
        auth_ok = (
            client.instance_url == "https://enterprise-test.my.salesforce.com" and
            headers.get("Authorization") == "Bearer 00DTESTBULKTOKEN" and
            headers.get("Content-Type") == "application/json; charset=UTF-8" and
            "X-Correlation-ID" in headers and
            "X-Request-ID" in headers
        )
        auth_detail = f"Instance: {client.instance_url} | Header: Bearer *** | Correlation: {headers.get('X-Correlation-ID')}"
    except Exception as exc:
        auth_ok = False
        auth_detail = f"Authentication setup error: {str(exc)}"
    checks.append(("Authentication & Client Integration", auth_ok, auth_detail))

    # 2. Create Bulk Query Job (POST /services/data/vXX.X/jobs/query)
    try:
        mock_create_resp = MagicMock(spec=requests.Response)
        mock_create_resp.status_code = 200
        mock_create_resp.json.return_value = {
            "id": "750300000001QJOB01",
            "operation": "query",
            "object": "Account",
            "state": "UploadComplete",
            "createdDate": "2026-11-01T09:00:00.000+0000"
        }

        with patch.object(client, "request", return_value=mock_create_resp) as mock_req:
            job_info = client.create_query_job("SELECT Id, Name, Industry FROM Account", target_object="Account")
            
            call_method = mock_req.call_args[0][0]
            call_path = mock_req.call_args[0][1]
            call_json = mock_req.call_args[1]["json"]

            creation_ok = (
                call_method == "POST" and
                call_path == "/services/data/v62.0/jobs/query" and
                call_json["operation"] == "query" and
                call_json["contentType"] == "CSV" and
                job_info["id"] == "750300000001QJOB01" and
                job_info["state"] == "UploadComplete"
            )
            creation_detail = f"Job ID: {job_info['id']} | State: {job_info['state']} | Endpoint: {call_path}"
    except Exception as exc:
        creation_ok = False
        creation_detail = f"Job creation error: {str(exc)}"
    checks.append(("Create Bulk Query Job (POST)", creation_ok, creation_detail))

    # 3. SOQL Syntax Pre-Validation
    try:
        invalid_caught = False
        try:
            client.create_query_job("INSERT INTO Account (Name) VALUES ('Acme')", target_object="Account")
        except InvalidSOQLError as soql_err:
            invalid_caught = (soql_err.status_code == 400 and soql_err.error_code == "INVALID_SOQL")
        except Exception:
            invalid_caught = False

        empty_caught = False
        try:
            client.create_query_job("", target_object="Account")
        except InvalidSOQLError:
            empty_caught = True
        except Exception:
            empty_caught = False

        soql_val_ok = invalid_caught and empty_caught
        soql_val_detail = "Non-SELECT and empty SOQL queries intercepted with InvalidSOQLError (400)"
    except Exception as exc:
        soql_val_ok = False
        soql_val_detail = f"SOQL validation error: {str(exc)}"
    checks.append(("SOQL Syntax Pre-Validation", soql_val_ok, soql_val_detail))

    # 4. Job Status & Real-time State Detection
    try:
        states = ["UploadComplete", "InProgress", "JobComplete", "Failed", "Aborted"]
        detected_states = []

        mock_status_resp = MagicMock(spec=requests.Response)
        mock_status_resp.status_code = 200

        poller = PollingService(salesforce_client=client)

        for s in states:
            mock_status_resp.json.return_value = {
                "id": "750300000001QJOB01",
                "state": s,
                "numberRecordsProcessed": 500 if s == "JobComplete" else 0
            }
            with patch.object(client, "request", return_value=mock_status_resp):
                status_data = client.get_job_status("750300000001QJOB01")
                detected = poller.detect_state(status_data)
                detected_states.append(detected)

        state_det_ok = (detected_states == states) and poller.is_job_complete("JobComplete") and poller.is_job_failed("Failed")
        state_det_detail = f"Detected all states: {', '.join(detected_states)}"
    except Exception as exc:
        state_det_ok = False
        state_det_detail = f"State detection error: {str(exc)}"
    checks.append(("Job Status & State Detection", state_det_ok, state_det_detail))

    # 5. Automatic Polling (Decorrelated Jitter & Stop on JobComplete)
    try:
        mock_poller_client = MagicMock(spec=SalesforceClient)
        mock_poller_client.get_job_status.side_effect = [
            {"id": "750300000001POLL01", "state": "UploadComplete", "numberRecordsProcessed": 0},
            {"id": "750300000001POLL01", "state": "InProgress", "numberRecordsProcessed": 250},
            {"id": "750300000001POLL01", "state": "JobComplete", "numberRecordsProcessed": 1000}
        ]

        test_poller = PollingService(salesforce_client=mock_poller_client)

        with patch("time.sleep", return_value=None):
            terminal_status = test_poller.poll_until_complete(
                "750300000001POLL01",
                base_interval=0.01,
                max_interval=0.05
            )

        polling_ok = (
            terminal_status["state"] == "JobComplete" and
            terminal_status["numberRecordsProcessed"] == 1000 and
            terminal_status["polling_attempts"] == 3 and
            mock_poller_client.get_job_status.call_count == 3
        )
        polling_detail = f"Progression: UploadComplete -> InProgress -> JobComplete in {terminal_status['polling_attempts']} attempts"
    except Exception as exc:
        polling_ok = False
        polling_detail = f"Automatic polling error: {str(exc)}"
    checks.append(("Automatic Polling (JobComplete)", polling_ok, polling_detail))

    # 6. Polling Transient Failure Resilience
    try:
        mock_transient_client = MagicMock(spec=SalesforceClient)
        mock_transient_client.get_job_status.side_effect = [
            SalesforceAPIError("Temporary 503 Gateway Glitch", status_code=503),
            {"id": "750300000001RETRY01", "state": "InProgress", "numberRecordsProcessed": 100},
            {"id": "750300000001RETRY01", "state": "JobComplete", "numberRecordsProcessed": 500}
        ]

        resilient_poller = PollingService(salesforce_client=mock_transient_client)
        with patch("time.sleep", return_value=None):
            recovered_res = resilient_poller.poll_until_complete("750300000001RETRY01", base_interval=0.01)

        resilience_ok = (
            recovered_res["state"] == "JobComplete" and
            mock_transient_client.get_job_status.call_count == 3
        )
        resilience_detail = "Transient 503 error absorbed gracefully -> Completed on retry"
    except Exception as exc:
        resilience_ok = False
        resilience_detail = f"Resilience error: {str(exc)}"
    checks.append(("Polling Transient Retry Resilience", resilience_ok, resilience_detail))

    # 7. Polling Timeout Handling
    try:
        mock_timeout_client = MagicMock(spec=SalesforceClient)
        mock_timeout_client.get_job_status.return_value = {"id": "750300000001TO01", "state": "InProgress"}

        timeout_poller = PollingService(salesforce_client=mock_timeout_client)

        clock = [100.0]
        def fake_clock():
            val = clock[0]
            clock[0] += 15.0
            return val

        with patch("time.time", side_effect=fake_clock), patch("time.sleep", return_value=None):
            timeout_raised = False
            try:
                timeout_poller.poll_until_complete("750300000001TO01", timeout_seconds=10)
            except BulkQueryTimeoutError as to_err:
                timeout_raised = (to_err.status_code == 504 and to_err.error_code == "QUERY_TIMEOUT")

        timeout_detail = "BulkQueryTimeoutError (504 GATEWAY_TIMEOUT) raised when exceeding timeout"
    except Exception as exc:
        timeout_raised = False
        timeout_detail = f"Timeout error: {str(exc)}"
    checks.append(("Polling Timeout Protection", timeout_raised, timeout_detail))

    # 8. Result Download & Paginated CSV Streaming (Sforce-Locator)
    try:
        mock_stream_client = MagicMock(spec=SalesforceClient)
        
        resp_chunk1 = MagicMock(spec=requests.Response)
        resp_chunk1.iter_lines.return_value = [
            "Id,Name,Industry,AnnualRevenue",
            "001300000001AAA001,Acme Corporation,Technology,1000000",
            "001300000001AAA002,Global Logistics,Transportation,500000"
        ]
        
        resp_chunk2 = MagicMock(spec=requests.Response)
        resp_chunk2.iter_lines.return_value = [
            "Id,Name,Industry,AnnualRevenue",
            "001300000001AAA003,Apex Healthcare,Healthcare,2500000"
        ]

        mock_stream_client.get_job_results_stream.side_effect = [
            (resp_chunk1, "LOCATOR_PAGE_2_CURSOR"),
            (resp_chunk2, None)
        ]

        stream_svc = CSVStreamService(salesforce_client=mock_stream_client)
        df_chunks = list(stream_svc.stream_csv_chunks("750300000001STRM01", chunk_size=2))

        stream_ok = (
            len(df_chunks) == 2 and
            len(df_chunks[0]) == 2 and
            len(df_chunks[1]) == 1 and
            df_chunks[0]["Name"].iloc[0] == "Acme Corporation" and
            df_chunks[1]["Name"].iloc[0] == "Apex Healthcare"
        )
        stream_detail = f"Streamed 2 locator chunks ({len(df_chunks[0]) + len(df_chunks[1])} total rows) maintaining O(1) heap"
    except Exception as exc:
        stream_ok = False
        stream_detail = f"Streaming error: {str(exc)}"
    checks.append(("Result Download & CSV Streaming", stream_ok, stream_detail))

    # 9. CSV Parsing & JSON Object Conversion
    try:
        # Re-mock stream for json records conversion test
        mock_stream_client.get_job_results_stream.side_effect = [
            (resp_chunk1, "LOCATOR_PAGE_2_CURSOR"),
            (resp_chunk2, None)
        ]
        json_records = stream_svc.download_all_records_as_json("750300000001STRM01")

        json_ok = (
            len(json_records) == 3 and
            isinstance(json_records[0], dict) and
            json_records[0]["Id"] == "001300000001AAA001" and
            json_records[0]["Name"] == "Acme Corporation" and
            json_records[2]["Name"] == "Apex Healthcare"
        )
        json_detail = f"Converted {len(json_records)} records into structured JSON dictionaries"
    except Exception as exc:
        json_ok = False
        json_detail = f"JSON conversion error: {str(exc)}"
    checks.append(("CSV to JSON Conversion & Structured Data", json_ok, json_detail))

    # 10. Job Management: Abort Query Job (PATCH)
    try:
        mock_abort_resp = MagicMock(spec=requests.Response)
        mock_abort_resp.status_code = 200
        mock_abort_resp.json.return_value = {
            "id": "750300000001ABORT01",
            "state": "Aborted"
        }

        with patch.object(client, "request", return_value=mock_abort_resp) as mock_abort_req:
            abort_res = client.abort_query_job("750300000001ABORT01")
            
            call_method = mock_abort_req.call_args[0][0]
            call_path = mock_abort_req.call_args[0][1]
            call_json = mock_abort_req.call_args[1]["json"]

            abort_ok = (
                call_method == "PATCH" and
                call_path == "/services/data/v62.0/jobs/query/750300000001ABORT01" and
                call_json == {"state": "Aborted"} and
                abort_res["state"] == "Aborted"
            )
            abort_detail = f"Dispatched PATCH state='Aborted' -> Confirmed: {abort_res['state']}"
    except Exception as exc:
        abort_ok = False
        abort_detail = f"Abort error: {str(exc)}"
    checks.append(("Job Management: Abort Job", abort_ok, abort_detail))

    # 11. Job Management: Delete/Cleanup Query Job (DELETE 204)
    try:
        mock_del_resp = MagicMock(spec=requests.Response)
        mock_del_resp.status_code = 204

        with patch.object(client, "request", return_value=mock_del_resp) as mock_del_req:
            del_res = client.delete_query_job("750300000001DEL01")
            
            call_method = mock_del_req.call_args[0][0]
            call_path = mock_del_req.call_args[0][1]

            cleanup_ok = (
                call_method == "DELETE" and
                call_path == "/services/data/v62.0/jobs/query/750300000001DEL01" and
                del_res["deleted"] is True and
                del_res["status"] == "DELETED"
            )
            cleanup_detail = f"Dispatched DELETE -> Handled HTTP 204 No Content -> Cleaned up"
    except Exception as exc:
        cleanup_ok = False
        cleanup_detail = f"Cleanup error: {str(exc)}"
    checks.append(("Job Management: Delete / Cleanup", cleanup_ok, cleanup_detail))

    # 12. Error Handling: 401 Session Refresh Retry & 429 Rate Limiting
    try:
        # Test 401 Auto-refresh
        resp_401 = MagicMock(spec=requests.Response)
        resp_401.status_code = 401
        resp_401.headers = {}
        resp_401.text = "Session expired or invalid"

        resp_200 = MagicMock(spec=requests.Response)
        resp_200.status_code = 200
        resp_200.headers = {"Sforce-Limit-Info": "api-usage=150/100000"}
        resp_200.json.return_value = {"id": "750300000001REF01", "state": "UploadComplete"}

        with patch.object(oauth, "authenticate") as mock_auth, \
             patch.object(oauth.session, "request", side_effect=[resp_401, resp_200]) as mock_req:
            retry_res = client.request("GET", "/services/data/v62.0/jobs/query/750300000001REF01")
            auto_401_ok = (retry_res.status_code == 200 and mock_auth.called and mock_req.call_count == 2)

        # Test 429 Rate Limit
        resp_429 = MagicMock(spec=requests.Response)
        resp_429.status_code = 429
        resp_429.headers = {}
        with patch.object(oauth.session, "request", return_value=resp_429):
            rate_limit_caught = False
            try:
                client.request("GET", "/services/data/v62.0/jobs/query")
            except RateLimitExceededError:
                rate_limit_caught = True

        err_handling_ok = auto_401_ok and rate_limit_caught
        err_handling_detail = "HTTP 401 triggers transparent token renewal; HTTP 429 triggers RateLimitExceededError"
    except Exception as exc:
        err_handling_ok = False
        err_handling_detail = f"Error handling error: {str(exc)}"
    checks.append(("Error Handling (401 Refresh & 429 Limits)", err_handling_ok, err_handling_detail))

    # 13. End-to-End Service Layer Orchestration (BulkService + DB)
    try:
        app = create_app("testing")
        with app.app_context():
            db.create_all()
            repo = BulkRepository()
            
            mock_sf = MagicMock(spec=SalesforceClient)
            mock_sf.api_version = "v62.0"
            mock_sf.create_query_job.return_value = {
                "id": "750300000001SRV01",
                "operation": "query",
                "object": "Account",
                "state": "UploadComplete"
            }
            mock_sf.get_job_status.return_value = {
                "id": "750300000001SRV01",
                "state": "JobComplete",
                "numberRecordsProcessed": 2
            }
            mock_sf.delete_query_job.return_value = {"id": "750300000001SRV01", "deleted": True}

            bulk_service = BulkService(salesforce_client=mock_sf, bulk_repo=repo)

            # Step 1: Create Job
            created_job = bulk_service.create_job("Account", "SELECT Id, Name FROM Account")
            local_job_id = created_job["job_id"]

            # Step 2: Poll Job
            polled_job = bulk_service.poll_job(local_job_id)

            # Step 3: Cleanup Job
            del_result = bulk_service.delete_job(local_job_id)

            # Verify local audit ledger state
            db_job = repo.get_by_id(local_job_id)
            logs = repo.session.query(AuditLog).filter_by(job_id=db_job.id).all()
            log_actions = [l.action for l in logs]

            service_ok = (
                db_job is not None and
                db_job.sf_job_id == "750300000001SRV01" and
                polled_job["status"] == IngestionStatus.COMPLETED.value and
                del_result["status"] == "DELETED" and
                "JOB_CREATED" in log_actions and
                "JOB_POLL_COMPLETED" in log_actions and
                "JOB_DELETED" in log_actions
            )
            service_detail = f"Lifecycle: Created -> Polled -> Cleaned up | Audit Actions: {', '.join(log_actions)}"
    except Exception as exc:
        service_ok = False
        service_detail = f"Service orchestration error: {str(exc)}"
    checks.append(("Service Orchestration & Audit Ledger", service_ok, service_detail))

    # Print summary table
    all_passed = True
    for name, status, detail in checks:
        status_str = "PASS" if status else "FAIL"
        if not status:
            all_passed = False
        print(f"[{status_str}] {name:<38} : {detail}")

    print("=" * 75)
    if all_passed:
        print("PHASE 6 SALESFORCE BULK API 2.0 VERIFIED")
        print("=" * 75)
        return True
    else:
        print("PHASE 6 SALESFORCE BULK API 2.0 FAILED")
        print("=" * 75)
        return False

if __name__ == "__main__":
    if not run_phase6_verification():
        sys.exit(1)
    sys.exit(0)

