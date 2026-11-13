"""
API Routes & Blueprint Registration.
Defines HTTP endpoints mapping directly to dedicated Controller actions.
"""

from flask import Blueprint
from app.controllers.auth_controller import AuthController
from app.controllers.bulk_controller import BulkController
from app.controllers.health_controller import HealthController
from app.controllers.orchestrator_controller import OrchestratorController

api_bp = Blueprint("api", __name__)

# Initialize Controllers
auth_ctrl = AuthController()
bulk_ctrl = BulkController()
health_ctrl = HealthController()
orchestrator_ctrl = OrchestratorController()

# ==============================================================================
# Health and Metrics Endpoints
# ==============================================================================
@api_bp.route("/health", methods=["GET"])
def health_check():
    """Liveness & readiness probe."""
    return health_ctrl.get_health()

@api_bp.route("/health/ready", methods=["GET"])
def readiness_check():
    """Kubernetes/Cloud readiness dependency probe."""
    return health_ctrl.get_readiness()

@api_bp.route("/health/live", methods=["GET"])
def liveness_check():
    """Kubernetes/Cloud liveness probe."""
    return health_ctrl.get_liveness()

@api_bp.route("/metrics", methods=["GET"])
def metrics():
    """Operational telemetry counters (JSON format)."""
    return health_ctrl.get_metrics()

@api_bp.route("/metrics/prometheus", methods=["GET"])
def prometheus_metrics():
    """Prometheus exposition text format metrics."""
    return health_ctrl.get_prometheus_metrics()

@api_bp.route("/dashboard", methods=["GET"])
def dashboard():
    """Enterprise operational monitoring dashboard snapshot."""
    return health_ctrl.get_dashboard()

# ==============================================================================
# Authentication Endpoints
# ==============================================================================
@api_bp.route("/login", methods=["POST"])
def login():
    """Salesforce OAuth 2.0 login."""
    return auth_ctrl.login()

@api_bp.route("/refresh", methods=["POST"])
def refresh_token():
    """Force renewal of OAuth session."""
    return auth_ctrl.refresh()

# ==============================================================================
# Bulk API 2.0 Management Endpoints
# ==============================================================================
@api_bp.route("/bulk/jobs", methods=["POST"])
def create_bulk_job():
    """Create a new Bulk API 2.0 query job."""
    return bulk_ctrl.create_job()

@api_bp.route("/bulk/jobs", methods=["GET"])
def list_bulk_jobs():
    """List historical ingestion jobs."""
    return bulk_ctrl.list_jobs()

@api_bp.route("/bulk/jobs/<string:job_id>", methods=["GET"])
def get_bulk_job(job_id: str):
    """Retrieve status of a specific Bulk API query job."""
    return bulk_ctrl.get_job(job_id=job_id)

@api_bp.route("/bulk/jobs/<string:job_id>/status", methods=["GET"])
def get_bulk_job_status(job_id: str):
    """Retrieve real-time status of a specific Bulk API query job."""
    return bulk_ctrl.get_job_status(job_id=job_id)

@api_bp.route("/bulk/jobs/<string:job_id>/poll", methods=["POST"])
def poll_bulk_job(job_id: str):
    """Poll Bulk API query job until JobComplete state."""
    return bulk_ctrl.poll_job(job_id=job_id)

@api_bp.route("/bulk/jobs/<string:job_id>/results", methods=["GET"])
def download_bulk_job_results(job_id: str):
    """Download results of a Bulk API query job as structured JSON."""
    return bulk_ctrl.download_results(job_id=job_id)

@api_bp.route("/bulk/jobs/<string:job_id>/start", methods=["POST"])
def start_bulk_job(job_id: str):
    """Start ingestion pipeline execution for an existing Bulk API job."""
    return bulk_ctrl.start_job(job_id=job_id)

@api_bp.route("/bulk/jobs/<string:job_id>/cancel", methods=["POST"])
def cancel_bulk_job(job_id: str):
    """Cancel an active Bulk API query job."""
    return bulk_ctrl.cancel_job(job_id=job_id)

@api_bp.route("/bulk/jobs/<string:job_id>", methods=["DELETE"])
def abort_bulk_job(job_id: str):
    """Abort an active query job."""
    return bulk_ctrl.abort_job(job_id=job_id)

@api_bp.route("/bulk/jobs/<string:job_id>/cleanup", methods=["DELETE"])
def cleanup_bulk_job(job_id: str):
    """Delete/cleanup a completed or aborted query job from Salesforce."""
    return bulk_ctrl.delete_job(job_id=job_id)

# ==============================================================================
# Ingestion Pipeline & Logs Endpoints
# ==============================================================================
@api_bp.route("/ingest", methods=["POST"])
def trigger_ingest():
    """Trigger complete ingestion pipeline."""
    return bulk_ctrl.trigger_ingest()

@api_bp.route("/logs", methods=["GET"])
def get_logs():
    """Retrieve application and dead-letter logs."""
    return bulk_ctrl.get_logs()

# ==============================================================================
# Phase 7: Orchestrator, DLQ & Scheduler Endpoints
# ==============================================================================
@api_bp.route("/orchestrator/jobs", methods=["POST"])
def submit_orchestrator_job():
    """Submit ingestion pipeline via orchestrator with optional async execution."""
    return orchestrator_ctrl.submit_job()

@api_bp.route("/orchestrator/recover", methods=["POST"])
def recover_interrupted_jobs():
    """Trigger workflow recovery for interrupted or orphaned jobs."""
    return orchestrator_ctrl.recover_jobs()

@api_bp.route("/dlq", methods=["GET"])
def list_dlq_jobs():
    """List quarantined Dead Letter Queue items."""
    return orchestrator_ctrl.list_dlq()

@api_bp.route("/dlq/<string:dlq_id>/replay", methods=["POST"])
def replay_dlq_job(dlq_id: str):
    """Replay a specific quarantined job from DLQ."""
    return orchestrator_ctrl.replay_dlq(dlq_id=dlq_id)

@api_bp.route("/dlq/replay-all", methods=["POST"])
def replay_all_dlq_jobs():
    """Replay all pending jobs from DLQ."""
    return orchestrator_ctrl.replay_all_dlq()

@api_bp.route("/dlq/cleanup", methods=["DELETE"])
def cleanup_dlq_jobs():
    """Purge expired items from Dead Letter Queue."""
    return orchestrator_ctrl.cleanup_dlq()

@api_bp.route("/scheduler/status", methods=["GET"])
def get_scheduler_status():
    """Retrieve scheduler health and registered job list."""
    return orchestrator_ctrl.get_scheduler_status()

@api_bp.route("/scheduler/jobs/<string:name>/trigger", methods=["POST"])
def trigger_scheduled_job(name: str):
    """Manually trigger immediate execution of a registered scheduled job."""
    return orchestrator_ctrl.trigger_scheduled_job(name=name)

@api_bp.route("/scheduler/pause", methods=["POST"])
def pause_scheduler():
    """Pause recurring scheduler."""
    return orchestrator_ctrl.pause_scheduler()

@api_bp.route("/scheduler/resume", methods=["POST"])
def resume_scheduler():
    """Resume recurring scheduler."""
    return orchestrator_ctrl.resume_scheduler()
