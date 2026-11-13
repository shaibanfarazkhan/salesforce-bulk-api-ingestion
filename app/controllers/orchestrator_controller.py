"""
Orchestrator, Resilience & Scheduling Controller.
Provides HTTP interface for orchestration pipeline dispatch, workflow recovery,
Dead Letter Queue (DLQ) operations, and background job scheduling.
"""

from typing import Tuple, Optional
from flask import request, Response
from app.services.orchestrator_service import BulkIngestionOrchestrator
from app.services.dlq_service import DeadLetterQueue
from app.services.scheduler_service import IngestionScheduler
from app.schemas.bulk_schema import BulkJobCreateSchema
from app.utils.helpers import ResponseBuilder
from app.utils.exceptions import ValidationError

class OrchestratorController:
    """
    Presentation controller for enterprise orchestrator, DLQ, and scheduler endpoints.
    """

    def __init__(
        self,
        orchestrator: Optional[BulkIngestionOrchestrator] = None,
        dlq: Optional[DeadLetterQueue] = None,
        scheduler: Optional[IngestionScheduler] = None
    ) -> None:
        self.orchestrator = orchestrator or BulkIngestionOrchestrator()
        self.dlq = dlq or self.orchestrator.dlq
        self.scheduler = scheduler or IngestionScheduler()
        self.job_schema = BulkJobCreateSchema()

    def submit_job(self) -> Tuple[Response, int]:
        """
        POST /orchestrator/jobs
        Submit an end-to-end ingestion workflow with optional asynchronous dispatch.
        Query param: ?async=true
        """
        json_data = request.get_json(silent=True) or {}
        errors = self.job_schema.validate(json_data)
        if errors:
            raise ValidationError(message="Invalid orchestration request", details=errors)

        target_obj = json_data.get("object", "Account")
        soql = json_data.get("query", "")
        async_mode = request.args.get("async", "false").lower() in ("true", "1", "yes")

        result = self.orchestrator.submit_ingestion(
            target_object=target_obj,
            query=soql,
            async_exec=async_mode
        )

        status_code = 202 if async_mode else 200
        return ResponseBuilder.success(
            data=result,
            message=f"Orchestration job dispatched ({'async' if async_mode else 'sync'})",
            status_code=status_code
        )

    def recover_jobs(self) -> Tuple[Response, int]:
        """
        POST /orchestrator/recover
        Scan for interrupted jobs in SUBMITTED, IN_PROGRESS, or STREAMING states and recover them.
        """
        reports = self.orchestrator.recover_interrupted_jobs()
        return ResponseBuilder.success(
            data={"recovered_jobs": reports, "count": len(reports)},
            message=f"Workflow recovery evaluated. {len(reports)} job(s) processed.",
            status_code=200
        )

    def list_dlq(self) -> Tuple[Response, int]:
        """
        GET /dlq
        List quarantined Dead Letter Queue items.
        """
        status = request.args.get("status")
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))

        items = self.dlq.list_items(status=status, limit=limit, offset=offset)
        return ResponseBuilder.success(
            data={"items": items, "count": len(items), "total_size": self.dlq.size()},
            message="Dead Letter Queue items retrieved successfully",
            status_code=200
        )

    def replay_dlq(self, dlq_id: str) -> Tuple[Response, int]:
        """
        POST /dlq/<dlq_id>/replay
        Replay a specific dead-letter job from DLQ.
        """
        result = self.dlq.replay_item(dlq_id=dlq_id, orchestrator=self.orchestrator)
        return ResponseBuilder.success(
            data=result,
            message=f"DLQ job '{dlq_id}' replayed successfully",
            status_code=200
        )

    def replay_all_dlq(self) -> Tuple[Response, int]:
        """
        POST /dlq/replay-all
        Replay all pending dead-letter jobs.
        """
        results = self.dlq.replay_all(orchestrator=self.orchestrator)
        return ResponseBuilder.success(
            data={"replayed_jobs": results, "count": len(results)},
            message=f"Replayed {len(results)} DLQ jobs",
            status_code=200
        )

    def cleanup_dlq(self) -> Tuple[Response, int]:
        """
        DELETE /dlq/cleanup
        Purge expired dead-letter items.
        """
        retention_days = request.args.get("retention_days", type=int)
        purged = self.dlq.cleanup_dlq(retention_days=retention_days)
        return ResponseBuilder.success(
            data={"purged_count": purged, "remaining_size": self.dlq.size()},
            message=f"DLQ cleanup complete. Purged {purged} items.",
            status_code=200
        )

    def get_scheduler_status(self) -> Tuple[Response, int]:
        """
        GET /scheduler/status
        Retrieve scheduler runtime health and registered job list.
        """
        status_data = self.scheduler.get_status()
        return ResponseBuilder.success(
            data=status_data,
            message="Scheduler status retrieved successfully",
            status_code=200
        )

    def trigger_scheduled_job(self, name: str) -> Tuple[Response, int]:
        """
        POST /scheduler/jobs/<name>/trigger
        Manually trigger immediate execution of a scheduled job.
        """
        result = self.scheduler.trigger_job_now(name=name)
        return ResponseBuilder.success(
            data=result,
            message=f"Scheduled job '{name}' triggered manually",
            status_code=200
        )

    def pause_scheduler(self) -> Tuple[Response, int]:
        """
        POST /scheduler/pause
        Pause recurring background schedule execution.
        """
        self.scheduler.pause()
        return ResponseBuilder.success(
            data={"status": "PAUSED"},
            message="Scheduler paused",
            status_code=200
        )

    def resume_scheduler(self) -> Tuple[Response, int]:
        """
        POST /scheduler/resume
        Resume background schedule execution.
        """
        self.scheduler.resume()
        return ResponseBuilder.success(
            data={"status": "RUNNING"},
            message="Scheduler resumed",
            status_code=200
        )

