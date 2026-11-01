"""
API Routes & Blueprint Registration.
Defines HTTP endpoints mapping directly to dedicated Controller actions.
"""

from flask import Blueprint
from app.controllers.auth_controller import AuthController
from app.controllers.bulk_controller import BulkController
from app.controllers.health_controller import HealthController

api_bp = Blueprint("api", __name__)

# Initialize Controllers
auth_ctrl = AuthController()
bulk_ctrl = BulkController()
health_ctrl = HealthController()

# ==============================================================================
# Health and Metrics Endpoints
# ==============================================================================
@api_bp.route("/health", methods=["GET"])
def health_check():
    """Liveness & readiness probe."""
    return health_ctrl.get_health()

@api_bp.route("/metrics", methods=["GET"])
def metrics():
    """Operational telemetry counters."""
    return health_ctrl.get_metrics()

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
