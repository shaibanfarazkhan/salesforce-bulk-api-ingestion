"""
Health and Telemetry Controller (Controller Layer).
Provides liveness, readiness, and runtime telemetry metric probes.
"""

from typing import Tuple
from flask import Response
from app.database.database import check_db_health
from app.utils.helpers import ResponseBuilder
from app.utils.telemetry import metrics_collector

class HealthController:
    """
    Controller handling health checks and operational telemetry metrics.
    """

    @staticmethod
    def get_health() -> Tuple[Response, int]:
        """
        GET /health
        Check database connection and system readiness.
        """
        db_status = check_db_health()
        overall_status = "UP" if db_status.get("status") == "UP" else "DEGRADED"
        
        health_payload = {
            "status": overall_status,
            "version": "1.0.0",
            "services": {
                "database": db_status,
                "salesforce_api": {"status": "INITIALIZED", "mode": "Bulk API 2.0"}
            }
        }
        status_code = 200 if overall_status == "UP" else 503
        return ResponseBuilder.success(
            data=health_payload,
            message="Health check executed successfully",
            status_code=status_code
        )

    @staticmethod
    def get_metrics() -> Tuple[Response, int]:
        """
        GET /metrics
        Retrieve telemetry performance indicators and counters.
        """
        metrics_payload = metrics_collector.get_metrics_snapshot()
        return ResponseBuilder.success(
            data=metrics_payload,
            message="Telemetry metrics retrieved successfully",
            status_code=200
        )
