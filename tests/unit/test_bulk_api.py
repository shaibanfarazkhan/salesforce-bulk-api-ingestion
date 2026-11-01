"""
Unit Tests for Salesforce Bulk API 2.0 Integration Module.
Comprehensive testing of Job creation, Decorrelated Jitter polling,
locator pagination, CSV streaming, schema validation, repository bulk upserts,
job aborting, and metrics collection with mocking.
"""

import io
import time
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest
import pandas as pd
import requests

from app.integrations.salesforce_client import SalesforceClient
from app.services.polling_service import PollingService
from app.services.csv_stream_service import CSVStreamService
from app.services.validation_service import ValidationService
from app.services.bulk_service import BulkService
from app.repositories.bulk_repository import BulkRepository
from app.database.models import IngestionJob, SalesforceAccount, SalesforceContact, SalesforceOpportunity
from app.utils.exceptions import (
    BulkJobException,
    PollingException,
    DownloadException,
    CSVException,
    ValidationException,
    ResourceNotFoundError
)
from app.utils.telemetry import metrics_collector

@pytest.fixture
def mock_client():
    """Mocked SalesforceClient."""
    client = MagicMock(spec=SalesforceClient)
    client.api_version = "v62.0"
    client.instance_url = "https://mock.salesforce.com"
    return client

# ==============================================================================
# 1. Bulk Query Job Creation Tests
# ==============================================================================
def test_create_query_job_success():
    """Verify Bulk API query job creation with valid SOQL."""
    client = SalesforceClient()
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "id": "750300000001AAA001",
        "operation": "query",
        "object": "Account",
        "state": "UploadComplete"
    }

    with patch.object(client, "request", return_value=mock_resp) as mock_req:
        result = client.create_query_job("SELECT Id, Name FROM Account", "Account")
        assert result["id"] == "750300000001AAA001"
        assert result["state"] == "UploadComplete"
        
        call_kwargs = mock_req.call_args[1]
        payload = call_kwargs["json"]
        assert payload["operation"] == "query"
        assert payload["contentType"] == "CSV"
        assert payload["lineEnding"] == "LF"
        assert payload["columnDelimiter"] == "COMMA"

def test_create_query_job_invalid_soql():
    """Verify non-SELECT queries are rejected before dispatch."""
    client = SalesforceClient()
    with pytest.raises(BulkJobException) as exc_info:
        client.create_query_job("UPDATE Account SET Name = 'Test'", "Account")
    assert "Invalid SOQL query expression" in str(exc_info.value)

# ==============================================================================
# 2. Status Polling Tests (Decorrelated Jitter Backoff)
# ==============================================================================
def test_polling_service_success(mock_client):
    """Verify polling loop terminates when job enters JobComplete."""
    poller = PollingService(salesforce_client=mock_client)
    mock_client.get_job_status.side_effect = [
        {"id": "7501", "state": "InProgress", "numberRecordsProcessed": 100},
        {"id": "7501", "state": "JobComplete", "numberRecordsProcessed": 1000}
    ]

    with patch("time.sleep", return_value=None):
        result = poller.poll_until_complete("7501", base_interval=0.01, max_interval=0.05)
        assert result["state"] == "JobComplete"
        assert result["numberRecordsProcessed"] == 1000
        assert mock_client.get_job_status.call_count == 2

def test_polling_service_failed_state(mock_client):
    """Verify polling loop raises PollingException on Failed state."""
    poller = PollingService(salesforce_client=mock_client)
    mock_client.get_job_status.return_value = {
        "id": "7501",
        "state": "Failed",
        "errorMessage": "QueryTimeout: SOQL chunk query exceeded 15 minutes"
    }

    with pytest.raises(PollingException) as exc_info:
        poller.poll_until_complete("7501", base_interval=0.01)
    assert "QueryTimeout" in str(exc_info.value)
    assert exc_info.value.status_code == 502

# ==============================================================================
# 3. CSV Streaming & Locator Pagination Tests
# ==============================================================================
def test_csv_streaming_with_locator_pagination(mock_client):
    """Verify streaming generator handles locator cursor transitions."""
    stream_service = CSVStreamService(salesforce_client=mock_client)

    # Response 1: returns first chunk with next locator
    resp1 = MagicMock(spec=requests.Response)
    resp1.iter_lines.return_value = [
        "Id,Name,SystemModstamp",
        "001300000001AAA001,Acme Corp,2026-10-24T20:00:00.000Z"
    ]
    # Response 2: returns second chunk with locator=None
    resp2 = MagicMock(spec=requests.Response)
    resp2.iter_lines.return_value = [
        "Id,Name,SystemModstamp",
        "001300000001BBB002,Global Logistics,2026-10-24T20:05:00.000Z"
    ]

    mock_client.get_job_results_stream.side_effect = [
        (resp1, "LOCATOR_CURSOR_XYZ"),
        (resp2, None)
    ]

    chunks = list(stream_service.stream_csv_chunks("7501", chunk_size=1000))
    assert len(chunks) == 2
    assert isinstance(chunks[0], pd.DataFrame)
    assert chunks[0]["Name"].iloc[0] == "Acme Corp"
    assert chunks[1]["Name"].iloc[0] == "Global Logistics"
    assert mock_client.get_job_results_stream.call_count == 2

# ==============================================================================
# 4. Data Cleansing & Validation Tests
# ==============================================================================
def test_validation_service_account_rules():
    """Verify validation partitions valid vs corrupt account rows."""
    validator = ValidationService()
    df_raw = pd.DataFrame([
        {
            "Id": "001300000001AAA001",
            "Name": "Valid Enterprise Corp",
            "AnnualRevenue": "1,500,000.50",
            "SystemModstamp": "2026-10-24T20:30:00.000Z"
        },
        {
            "Id": "001300000001AAA002",
            "Name": "",  # Missing Name -> Must be rejected
            "AnnualRevenue": "500000",
            "SystemModstamp": "2026-10-24T20:30:00.000Z"
        },
        {
            "Id": "INVALID_SF_ID_123",  # Bad ID format
            "Name": "Invalid ID Corp",
            "SystemModstamp": "2026-10-24T20:30:00.000Z"
        },
        {
            "Id": "001300000001AAA001",  # Duplicate ID in same batch
            "Name": "Duplicate Acme",
            "SystemModstamp": "2026-10-24T20:30:00.000Z"
        }
    ])

    accepted, rejected = validator.sanitize_and_validate(df_raw, "Account")
    assert len(accepted) == 1
    assert accepted[0]["salesforce_id"] == "001300000001AAA001"
    assert accepted[0]["annual_revenue"] == Decimal("1500000.50")
    assert len(rejected) == 3

def test_validation_service_contact_and_opportunity():
    """Verify contact and opportunity specific validation."""
    validator = ValidationService()
    
    # Contact Chunk
    df_contact = pd.DataFrame([{
        "Id": "003300000001AAA101",
        "AccountId": "001300000001AAA001",
        "FirstName": "John",
        "LastName": "Doe",
        "Email": "john.doe@enterprise.com",
        "SystemModstamp": "2026-10-24T20:30:00Z"
    }])
    acc_c, rej_c = validator.sanitize_and_validate(df_contact, "Contact")
    assert len(acc_c) == 1
    assert acc_c[0]["last_name"] == "Doe"
    assert acc_c[0]["email"] == "john.doe@enterprise.com"

    # Opportunity Chunk
    df_opp = pd.DataFrame([{
        "Id": "006300000001AAA201",
        "AccountId": "001300000001AAA001",
        "Name": "Cloud Migration Q4",
        "StageName": "Closed Won",
        "Amount": "750000",
        "Probability": "100",
        "CloseDate": "2026-12-31",
        "SystemModstamp": "2026-10-24T20:30:00Z"
    }])
    acc_o, rej_o = validator.sanitize_and_validate(df_opp, "Opportunity")
    assert len(acc_o) == 1
    assert acc_o[0]["amount"] == Decimal("750000")
    assert acc_o[0]["probability"] == Decimal("100")

# ==============================================================================
# 5. Repository Layer & Upsert Tests
# ==============================================================================
def test_repository_bulk_upsert_accounts(app):
    """Verify repository ON CONFLICT DO UPDATE upserts with deadlock sorting."""
    with app.app_context():
        repo = BulkRepository()
        records = [
            {
                "salesforce_id": "001300000001TEST01",
                "name": "Test Upsert Account 1",
                "annual_revenue": Decimal("250000.00"),
                "system_modstamp": datetime.now(timezone.utc),
                "is_deleted": False,
                "synced_at": datetime.now(timezone.utc)
            },
            {
                "salesforce_id": "001300000001TEST02",
                "name": "Test Upsert Account 2",
                "annual_revenue": Decimal("750000.00"),
                "system_modstamp": datetime.now(timezone.utc),
                "is_deleted": False,
                "synced_at": datetime.now(timezone.utc)
            }
        ]

        # Initial Insert
        inserted = repo.bulk_upsert_accounts(records)
        assert inserted == 2

        # Verify query
        acc1 = repo.session.query(SalesforceAccount).filter_by(salesforce_id="001300000001TEST01").first()
        assert acc1 is not None
        assert acc1.name == "Test Upsert Account 1"

        # Update via Conflict
        records[0]["name"] = "Updated Account 1 Name"
        updated = repo.bulk_upsert_accounts(records)
        assert updated == 2
        acc1_updated = repo.session.query(SalesforceAccount).filter_by(salesforce_id="001300000001TEST01").first()
        assert acc1_updated.name == "Updated Account 1 Name"

# ==============================================================================
# 6. End-to-End BulkService Ingestion Pipeline Test
# ==============================================================================
def test_bulk_service_ingestion_pipeline_e2e(app, mock_client):
    """Verify complete end-to-end ingestion pipeline coordination."""
    with app.app_context():
        repo = BulkRepository()
        bulk_service = BulkService(salesforce_client=mock_client, bulk_repo=repo)

        # 1. Create Job Mock
        mock_client.create_query_job.return_value = {
            "id": "750300000001E2E001",
            "operation": "query",
            "object": "Account",
            "state": "UploadComplete"
        }

        job_info = bulk_service.create_job("Account", "SELECT Id, Name, SystemModstamp FROM Account")
        job_id = job_info["job_id"]
        assert job_info["status"] == "SUBMITTED"

        # 2. Poller Mock
        mock_client.get_job_status.return_value = {
            "id": "750300000001E2E001",
            "state": "JobComplete",
            "numberRecordsProcessed": 1
        }

        # 3. Stream Results Mock
        resp = MagicMock(spec=requests.Response)
        resp.iter_lines.return_value = [
            "Id,Name,SystemModstamp",
            "001300000001E2EACC,E2E Corporation,2026-10-24T20:30:00Z"
        ]
        mock_client.get_job_results_stream.return_value = (resp, None)

        # 4. Execute Ingestion Pipeline
        with patch("time.sleep", return_value=None):
            summary = bulk_service.start_ingestion(job_id)
            assert summary["status"] == "COMPLETED"
            assert summary["records_ingested"] == 1
            assert summary["records_failed"] == 0

        # Verify DB Persistence
        acc = repo.session.query(SalesforceAccount).filter_by(salesforce_id="001300000001E2EACC").first()
        assert acc is not None
        assert acc.name == "E2E Corporation"

# ==============================================================================
# 7. Job Cancellation Tests
# ==============================================================================
def test_bulk_service_abort_job(app, mock_client):
    """Verify job cancellation dispatches PATCH and updates audit record."""
    with app.app_context():
        repo = BulkRepository()
        bulk_service = BulkService(salesforce_client=mock_client, bulk_repo=repo)

        # Create job
        mock_client.create_query_job.return_value = {
            "id": "750300000001CAN001",
            "operation": "query",
            "object": "Account"
        }
        job_info = bulk_service.create_job("Account", "SELECT Id FROM Account")
        job_id = job_info["job_id"]

        # Abort job
        mock_client.abort_query_job.return_value = {"id": "750300000001CAN001", "state": "Aborted"}
        result = bulk_service.abort_job(job_id)
        assert result["status"] == "ABORTED"

        job = repo.get_by_id(job_id)
        assert job.status == "ABORTED"
        mock_client.abort_query_job.assert_called_once_with(sf_job_id="750300000001CAN001")

# ==============================================================================
# 8. Phase 6 Bulk API 2.0 Feature Tests (Cleanup, Polling, Results JSON)
# ==============================================================================
def test_client_delete_query_job_success():
    """Verify Bulk API DELETE query job sends HTTP DELETE and handles 204."""
    client = SalesforceClient()
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = 204

    with patch.object(client, "request", return_value=mock_resp) as mock_req:
        result = client.delete_query_job("750300000001DEL001")
        assert result["deleted"] is True
        assert result["status"] == "DELETED"
        mock_req.assert_called_once_with("DELETE", "/services/data/v62.0/jobs/query/750300000001DEL001")

def test_polling_service_transient_failure_recovery(mock_client):
    """Verify polling recovers from transient 503 error and finishes upon JobComplete."""
    from app.utils.exceptions import SalesforceAPIError
    poller = PollingService(salesforce_client=mock_client)
    mock_client.get_job_status.side_effect = [
        SalesforceAPIError("Transient 503 Service Unavailable", status_code=503),
        {"id": "7501", "state": "InProgress", "numberRecordsProcessed": 50},
        {"id": "7501", "state": "JobComplete", "numberRecordsProcessed": 100}
    ]

    with patch("time.sleep", return_value=None):
        result = poller.poll_until_complete("7501", base_interval=0.01, max_interval=0.05)
        assert result["state"] == "JobComplete"
        assert result["numberRecordsProcessed"] == 100
        assert mock_client.get_job_status.call_count == 3

def test_polling_service_timeout(mock_client):
    """Verify polling raises BulkQueryTimeoutError when timeout is reached."""
    from app.utils.exceptions import BulkQueryTimeoutError
    poller = PollingService(salesforce_client=mock_client)
    mock_client.get_job_status.return_value = {"id": "7501", "state": "InProgress"}

    clock = [100.0]
    def fake_time():
        val = clock[0]
        clock[0] += 6.0
        return val

    with patch("time.time", side_effect=fake_time), patch("time.sleep", return_value=None):
        with pytest.raises(BulkQueryTimeoutError) as exc_info:
            poller.poll_until_complete("7501", timeout_seconds=10)
        assert "Polling exceeded timeout threshold" in str(exc_info.value)

def test_csv_stream_to_json_records(mock_client):
    """Verify CSV results streaming converted directly to structured JSON objects."""
    stream_service = CSVStreamService(salesforce_client=mock_client)
    resp1 = MagicMock(spec=requests.Response)
    resp1.iter_lines.return_value = [
        "Id,Name,Type",
        "001001,Acme Corp,Customer",
        "001002,Global Tech,Partner"
    ]
    resp2 = MagicMock(spec=requests.Response)
    resp2.iter_lines.return_value = [
        "Id,Name,Type",
        "001003,Stark Industries,Enterprise"
    ]

    mock_client.get_job_results_stream.side_effect = [
        (resp1, "LOCATOR_PAGE_2"),
        (resp2, None)
    ]

    records = stream_service.download_all_records_as_json("7501", chunk_size=2)
    assert len(records) == 3
    assert records[0]["Name"] == "Acme Corp"
    assert records[1]["Name"] == "Global Tech"
    assert records[2]["Name"] == "Stark Industries"

def test_bulk_service_delete_cleanup_job(app, mock_client):
    """Verify BulkService coordinates delete job cleanup and audit logging."""
    with app.app_context():
        repo = BulkRepository()
        bulk_service = BulkService(salesforce_client=mock_client, bulk_repo=repo)

        mock_client.create_query_job.return_value = {
            "id": "750300000001CLN001",
            "operation": "query",
            "object": "Account"
        }
        job_info = bulk_service.create_job("Account", "SELECT Id FROM Account")
        job_id = job_info["job_id"]

        mock_client.delete_query_job.return_value = {"id": "750300000001CLN001", "deleted": True}
        cleanup_res = bulk_service.delete_job(job_id)
        assert cleanup_res["status"] == "DELETED"
        mock_client.delete_query_job.assert_called_once_with(sf_job_id="750300000001CLN001")
