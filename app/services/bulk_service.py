"""
Salesforce Bulk API 2.0 Ingestion Orchestrator Service.
Coordinates the end-to-end ingestion lifecycle:
1. Job Submission (SOQL Query Validation)
2. Database Audit Initialization
3. Decorrelated Jitter Status Polling
4. Memory-Bounded Stream Download (Sforce-Locator Pagination)
5. Schema Validation & Dead-Letter Quarantine
6. Idempotent PostgreSQL Upserts (ON CONFLICT DO UPDATE)
7. Operational Telemetry & Job Cancellation
"""

import time
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import uuid

from app.integrations.salesforce_client import SalesforceClient
from app.repositories.bulk_repository import BulkRepository
from app.services.polling_service import PollingService
from app.services.csv_stream_service import CSVStreamService
from app.services.validation_service import ValidationService
from app.database.models import IngestionJob, AuditLog
from app.utils.constants import IngestionStatus, SalesforceJobState
from app.utils.exceptions import BulkJobException, ResourceNotFoundError
from app.utils.telemetry import metrics_collector

logger = logging.getLogger(__name__)

class BulkService:
    """
    Master business service orchestrating Bulk API 2.0 query lifecycle and ingestion pipelines.
    """

    def __init__(
        self,
        salesforce_client: Optional[SalesforceClient] = None,
        bulk_repo: Optional[BulkRepository] = None,
        polling_service: Optional[PollingService] = None,
        stream_service: Optional[CSVStreamService] = None,
        validation_service: Optional[ValidationService] = None
    ) -> None:
        self.salesforce_client = salesforce_client or SalesforceClient()
        self.bulk_repo = bulk_repo or BulkRepository()
        self.polling_service = polling_service or PollingService(salesforce_client=self.salesforce_client)
        self.stream_service = stream_service or CSVStreamService(salesforce_client=self.salesforce_client)
        self.validation_service = validation_service or ValidationService()

    def create_job(self, target_object: str, query: str) -> Dict[str, Any]:
        """
        Create a new Bulk API 2.0 query job on Salesforce and initialize local audit ledger.
        """
        metrics_collector.record_job_started()

        # 1. Create Job in Salesforce
        sf_job = self.salesforce_client.create_query_job(soql_query=query, target_object=target_object)
        sf_job_id = sf_job.get("id")

        # 2. Persist IngestionJob Audit Record
        job_entity = IngestionJob(
            sf_job_id=sf_job_id,
            target_object=target_object,
            operation="query",
            soql_query=query,
            status=IngestionStatus.SUBMITTED.value,
            records_processed=0,
            records_ingested=0,
            records_failed=0,
            started_at=datetime.now(timezone.utc)
        )
        saved_job = self.bulk_repo.create_job(job_entity)

        # 3. Insert Audit Log
        self.bulk_repo.insert_logs([{
            "job_id": saved_job.id,
            "action": "JOB_CREATED",
            "entity_type": target_object,
            "record_count": 0,
            "message": f"Bulk API query job {sf_job_id} submitted successfully.",
            "payload": {"sf_job_id": sf_job_id, "query": query}
        }])

        return {
            "job_id": str(saved_job.id),
            "sf_job_id": sf_job_id,
            "target_object": target_object,
            "status": saved_job.status,
            "operation": saved_job.operation,
            "created_at": saved_job.created_at.isoformat()
        }

    def start_ingestion(self, job_identifier: str, chunk_size: int = 10000) -> Dict[str, Any]:
        """
        Execute end-to-end ingestion pipeline:
        Poll status -> Stream CSV chunks -> Validate records -> Bulk upsert -> Record telemetry.
        """
        job = self._find_job(job_identifier)
        sf_job_id = job.sf_job_id

        pipeline_start = time.time()
        logger.info(f"Starting ingestion pipeline for job {job.id} (Salesforce ID: {sf_job_id})...")

        try:
            # 1. Update status to IN_PROGRESS
            job.status = IngestionStatus.IN_PROGRESS.value
            job.started_at = datetime.now(timezone.utc)
            self.bulk_repo.update_job(job)

            # 2. Poll status until complete
            terminal_status = self.polling_service.poll_until_complete(sf_job_id=sf_job_id)
            total_records_in_sf = terminal_status.get("numberRecordsProcessed", 0)
            job.records_processed = total_records_in_sf

            # 3. Stream and Process CSV Chunks
            job.status = IngestionStatus.STREAMING.value
            self.bulk_repo.update_job(job)

            total_ingested = 0
            total_quarantined = 0
            target_obj = job.target_object.lower()

            for df_chunk in self.stream_service.stream_csv_chunks(sf_job_id=sf_job_id, chunk_size=chunk_size):
                accepted, rejected = self.validation_service.sanitize_and_validate(
                    df_chunk=df_chunk,
                    target_object=job.target_object
                )

                # Persist rejected records to dead-letter quarantine
                if rejected:
                    for err in rejected:
                        err["job_id"] = job.id
                    self.bulk_repo.insert_errors(rejected)
                    total_quarantined += len(rejected)

                # Persist accepted records via idempotent upsert
                if accepted:
                    if "account" in target_obj:
                        upserted = self.bulk_repo.bulk_upsert_accounts(accepted)
                    elif "contact" in target_obj:
                        upserted = self.bulk_repo.bulk_upsert_contacts(accepted)
                    elif "opportunity" in target_obj:
                        upserted = self.bulk_repo.bulk_upsert_opportunities(accepted)
                    else:
                        upserted = len(accepted)

                    total_ingested += upserted
                    metrics_collector.record_rows_processed(upserted)

            # 4. Mark Pipeline COMPLETED
            pipeline_duration_ms = int((time.time() - pipeline_start) * 1000)
            job.status = IngestionStatus.COMPLETED.value
            job.records_ingested = total_ingested
            job.records_failed = total_quarantined
            job.execution_time_ms = pipeline_duration_ms
            job.completed_at = datetime.now(timezone.utc)
            self.bulk_repo.update_job(job)

            metrics_collector.record_job_completed()

            # 5. Insert Final Audit Log
            self.bulk_repo.insert_logs([{
                "job_id": job.id,
                "action": "INGESTION_COMPLETED",
                "entity_type": job.target_object,
                "record_count": total_ingested,
                "message": f"Pipeline completed in {pipeline_duration_ms}ms: {total_ingested} ingested, {total_quarantined} quarantined.",
                "payload": {"duration_ms": pipeline_duration_ms, "ingested": total_ingested, "quarantined": total_quarantined}
            }])

            logger.info(
                f"Ingestion pipeline completed for job {job.id}: "
                f"{total_ingested} records upserted, {total_quarantined} quarantined in {pipeline_duration_ms}ms"
            )

            return {
                "job_id": str(job.id),
                "sf_job_id": job.sf_job_id,
                "target_object": job.target_object,
                "status": job.status,
                "records_processed": job.records_processed,
                "records_ingested": job.records_ingested,
                "records_failed": job.records_failed,
                "execution_time_ms": job.execution_time_ms,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None
            }

        except Exception as exc:
            metrics_collector.record_job_failed()
            logger.error(f"Ingestion pipeline failed for job {job.id}: {str(exc)}")
            job.status = IngestionStatus.FAILED.value
            job.error_message = str(exc)
            job.completed_at = datetime.now(timezone.utc)
            job.execution_time_ms = int((time.time() - pipeline_start) * 1000)
            self.bulk_repo.update_job(job)

            self.bulk_repo.insert_logs([{
                "job_id": job.id,
                "action": "INGESTION_FAILED",
                "entity_type": job.target_object,
                "record_count": 0,
                "message": f"Pipeline failed: {str(exc)}",
                "payload": {"error": str(exc)}
            }])
            raise

    def abort_job(self, job_identifier: str) -> Dict[str, Any]:
        """
        Abort an active Bulk API query job on Salesforce and mark local record as ABORTED.
        """
        job = self._find_job(job_identifier)
        sf_job_id = job.sf_job_id

        # Abort on Salesforce
        abort_resp = self.salesforce_client.abort_query_job(sf_job_id=sf_job_id)

        # Update local audit ledger
        job.status = IngestionStatus.ABORTED.value
        job.completed_at = datetime.now(timezone.utc)
        self.bulk_repo.update_job(job)

        self.bulk_repo.insert_logs([{
            "job_id": job.id,
            "action": "JOB_ABORTED",
            "entity_type": job.target_object,
            "record_count": 0,
            "message": f"Bulk job {sf_job_id} aborted by client request.",
            "payload": abort_resp
        }])

        return {
            "job_id": str(job.id),
            "sf_job_id": sf_job_id,
            "status": IngestionStatus.ABORTED.value,
            "message": "Bulk query job successfully aborted."
        }

    def delete_job(self, job_identifier: str) -> Dict[str, Any]:
        """
        Delete/cleanup a Bulk API query job on Salesforce and record audit log.
        """
        job = self._find_job(job_identifier)
        sf_job_id = job.sf_job_id

        # Delete on Salesforce
        del_resp = self.salesforce_client.delete_query_job(sf_job_id=sf_job_id)

        self.bulk_repo.insert_logs([{
            "job_id": job.id,
            "action": "JOB_DELETED",
            "entity_type": job.target_object,
            "record_count": 0,
            "message": f"Bulk job {sf_job_id} deleted for cleanup.",
            "payload": del_resp
        }])

        return {
            "job_id": str(job.id),
            "sf_job_id": sf_job_id,
            "status": "DELETED",
            "message": f"Bulk query job {sf_job_id} successfully cleaned up."
        }

    def poll_job(
        self,
        job_identifier: str,
        base_interval: float = 2.0,
        max_interval: float = 30.0,
        timeout_seconds: int = 900
    ) -> Dict[str, Any]:
        """
        Poll Bulk API query job until completion and update internal audit state.
        """
        job = self._find_job(job_identifier)
        sf_job_id = job.sf_job_id

        logger.info(f"Polling job {job.id} (sf_job_id={sf_job_id}) until completion...")
        polled_status = self.polling_service.poll_until_complete(
            sf_job_id=sf_job_id,
            base_interval=base_interval,
            max_interval=max_interval,
            timeout_seconds=timeout_seconds
        )

        records_processed = polled_status.get("numberRecordsProcessed", 0)
        job.records_processed = records_processed
        job.status = IngestionStatus.COMPLETED.value
        job.completed_at = datetime.now(timezone.utc)
        self.bulk_repo.update_job(job)

        self.bulk_repo.insert_logs([{
            "job_id": job.id,
            "action": "JOB_POLL_COMPLETED",
            "entity_type": job.target_object,
            "record_count": records_processed,
            "message": f"Bulk API query job {sf_job_id} reached terminal JobComplete status.",
            "payload": polled_status
        }])

        return {
            "job_id": str(job.id),
            "sf_job_id": sf_job_id,
            "target_object": job.target_object,
            "status": job.status,
            "records_processed": records_processed,
            "total_polling_time_seconds": polled_status.get("total_polling_time_seconds"),
            "polling_attempts": polled_status.get("polling_attempts")
        }

    def download_job_results(
        self,
        job_identifier: str,
        max_records: Optional[int] = None,
        chunk_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Download CSV results for a Bulk API query job and return structured JSON records.
        """
        job = self._find_job(job_identifier)
        sf_job_id = job.sf_job_id

        logger.info(f"Downloading query results for job {job.id} (sf_job_id={sf_job_id})...")
        records = self.stream_service.download_all_records_as_json(
            sf_job_id=sf_job_id,
            max_records=max_records,
            chunk_size=chunk_size
        )

        self.bulk_repo.insert_logs([{
            "job_id": job.id,
            "action": "RESULTS_DOWNLOADED",
            "entity_type": job.target_object,
            "record_count": len(records),
            "message": f"Downloaded {len(records)} records for job {sf_job_id}.",
            "payload": {"record_count": len(records)}
        }])

        return {
            "job_id": str(job.id),
            "sf_job_id": sf_job_id,
            "target_object": job.target_object,
            "record_count": len(records),
            "records": records
        }

    def get_job_status(self, job_identifier: str) -> Dict[str, Any]:
        """Retrieve real-time status from Salesforce combined with local audit metrics."""
        job = self._find_job(job_identifier)
        sf_status = {}
        try:
            sf_status = self.salesforce_client.get_job_status(job.sf_job_id)
        except Exception as exc:
            logger.warning(f"Could not retrieve live Salesforce status for {job.sf_job_id}: {str(exc)}")

        return {
            "job_id": str(job.id),
            "sf_job_id": job.sf_job_id,
            "target_object": job.target_object,
            "db_status": job.status,
            "sf_state": sf_status.get("state", "UNKNOWN"),
            "records_processed": job.records_processed,
            "records_ingested": job.records_ingested,
            "records_failed": job.records_failed,
            "execution_time_ms": job.execution_time_ms,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None
        }

    def list_jobs(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """List historical bulk jobs from local database audit ledger."""
        jobs = self.bulk_repo.list_jobs(limit=limit, offset=offset)
        return [
            {
                "job_id": str(j.id),
                "sf_job_id": j.sf_job_id,
                "target_object": j.target_object,
                "status": j.status,
                "records_processed": j.records_processed,
                "records_ingested": j.records_ingested,
                "records_failed": j.records_failed,
                "created_at": j.created_at.isoformat() if j.created_at else None
            }
            for j in jobs
        ]

    def _find_job(self, identifier: str) -> IngestionJob:
        """Find an IngestionJob by internal UUID or Salesforce 750... job ID."""
        job = None
        # Try UUID first
        try:
            val_uuid = uuid.UUID(identifier)
            job = self.bulk_repo.get_by_id(val_uuid)
        except (ValueError, TypeError):
            pass

        if not job:
            job = self.bulk_repo.get_by_sf_job_id(identifier)

        if not job:
            raise ResourceNotFoundError(f"Bulk ingestion job not found for identifier: '{identifier}'")
        return job
