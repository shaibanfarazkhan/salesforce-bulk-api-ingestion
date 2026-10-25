"""
Bulk API 2.0 Management Controller (Presentation Layer).
Translates incoming HTTP requests into typed Service layer executions for
Bulk query creation, execution pipeline triggers, status queries, and cancellations.
"""

from typing import Tuple
from flask import request, Response
from app.services.bulk_service import BulkService
from app.schemas.bulk_schema import BulkJobCreateSchema, IngestRequestSchema
from app.utils.helpers import ResponseBuilder
from app.utils.exceptions import ValidationError

class BulkController:
    """
    Controller handling Bulk API 2.0 query lifecycle and ingestion endpoints.
    """

    def __init__(self, bulk_service: BulkService = None) -> None:
        self.bulk_service = bulk_service or BulkService()
        self.job_schema = BulkJobCreateSchema()
        self.ingest_schema = IngestRequestSchema()

    def create_job(self) -> Tuple[Response, int]:
        """
        POST /bulk/jobs
        Submit a new Bulk API 2.0 query job.
        """
        json_data = request.get_json(silent=True) or {}
        errors = self.job_schema.validate(json_data)
        if errors:
            raise ValidationError(message="Invalid job creation request", details=errors)

        target_object = json_data.get("object", "Account")
        query = json_data.get("query", "")
        job_result = self.bulk_service.create_job(target_object=target_object, query=query)

        return ResponseBuilder.success(
            data=job_result,
            message="Salesforce Bulk API 2.0 query job created successfully",
            status_code=201
        )

    def list_jobs(self) -> Tuple[Response, int]:
        """
        GET /bulk/jobs
        Retrieve paginated list of historical bulk ingestion jobs.
        """
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))
        jobs = self.bulk_service.list_jobs(limit=limit, offset=offset)
        return ResponseBuilder.success(
            data={"jobs": jobs, "count": len(jobs)},
            message="Bulk ingestion jobs retrieved successfully",
            status_code=200
        )

    def get_job(self, job_id: str) -> Tuple[Response, int]:
        """
        GET /bulk/jobs/<id>
        Inspect real-time status and audit details of a specific job.
        """
        job_status = self.bulk_service.get_job_status(job_identifier=job_id)
        return ResponseBuilder.success(
            data=job_status,
            message=f"Job status for '{job_id}' retrieved successfully",
            status_code=200
        )

    def start_job(self, job_id: str) -> Tuple[Response, int]:
        """
        POST /bulk/jobs/<id>/start
        Trigger the asynchronous polling, streaming download, and database upsert pipeline.
        """
        chunk_size = int(request.args.get("chunk_size", 10000))
        summary = self.bulk_service.start_ingestion(job_identifier=job_id, chunk_size=chunk_size)
        return ResponseBuilder.success(
            data=summary,
            message=f"Bulk ingestion pipeline executed successfully for job '{job_id}'",
            status_code=200
        )

    def cancel_job(self, job_id: str) -> Tuple[Response, int]:
        """
        POST /bulk/jobs/<id>/cancel
        Abort an active Bulk API query job.
        """
        result = self.bulk_service.abort_job(job_identifier=job_id)
        return ResponseBuilder.success(
            data=result,
            message=f"Job '{job_id}' aborted successfully",
            status_code=200
        )

    def get_job_status(self, job_id: str) -> Tuple[Response, int]:
        """
        GET /bulk/jobs/<id>/status
        Convenience alias to query job status.
        """
        return self.get_job(job_id=job_id)

    def trigger_ingest(self) -> Tuple[Response, int]:
        """
        POST /ingest
        Convenience endpoint to create a job and immediately run the ingestion pipeline.
        """
        json_data = request.get_json(silent=True) or {}
        errors = self.ingest_schema.validate(json_data)
        if errors:
            raise ValidationError(message="Invalid ingestion request parameters", details=errors)

        target_obj = json_data.get("object", "Account")
        soql = json_data.get("soql", "")
        batch_size = json_data.get("batch_size", 10000)

        # 1. Create Job
        created = self.bulk_service.create_job(target_object=target_obj, query=soql)
        job_id = created.get("job_id")

        # 2. Start Ingestion
        summary = self.bulk_service.start_ingestion(job_identifier=job_id, chunk_size=batch_size)

        return ResponseBuilder.success(
            data=summary,
            message="End-to-end Bulk API ingestion completed successfully",
            status_code=200
        )

    def get_logs(self) -> Tuple[Response, int]:
        """
        GET /logs
        Retrieve recent historical jobs and dead-letter count summary.
        """
        jobs = self.bulk_service.list_jobs(limit=10)
        return ResponseBuilder.success(
            data={"recent_jobs": jobs},
            message="Application logs retrieved successfully",
            status_code=200
        )
