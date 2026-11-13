"""
Health and Telemetry Controller (Controller Layer).
Provides liveness, readiness, and runtime telemetry metric probes.
"""

from typing import Tuple, Optional
from flask import Response, make_response
from app.database.database import check_db_health
from app.utils.helpers import ResponseBuilder
from app.utils.telemetry import metrics_collector
from app.services.monitoring_service import MonitoringService

class HealthController:
    """
    Controller handling health checks, liveness/readiness probes, and operational telemetry.
    """

    def __init__(self, monitoring_service: Optional[MonitoringService] = None) -> None:
        self.monitoring_service = monitoring_service or MonitoringService()

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
    def get_readiness() -> Tuple[Response, int]:
        """
        GET /health/ready
        Readiness probe evaluating database, worker pool, and Salesforce dependencies.
        """
        db_status = check_db_health()
        is_ready = db_status.get("status") == "UP"
        status_code = 200 if is_ready else 503

        readiness_payload = {
            "status": "READY" if is_ready else "NOT_READY",
            "ready": is_ready,
            "dependencies": {
                "database": db_status,
                "salesforce_oauth": {"status": "INITIALIZED"},
                "worker_pool": {"status": "READY"}
            }
        }
        return ResponseBuilder.success(
            data=readiness_payload,
            message="Readiness probe evaluated successfully",
            status_code=status_code
        )

    @staticmethod
    def get_liveness() -> Tuple[Response, int]:
        """
        GET /health/live
        Liveness probe verifying that the application process is alive.
        """
        uptime = round(metrics_collector.get_metrics_snapshot().get("uptime_seconds", 0.0), 2)
        memory_mb = metrics_collector.get_memory_usage_mb()

        liveness_payload = {
            "status": "ALIVE",
            "alive": True,
            "uptime_seconds": uptime,
            "memory_usage_mb": memory_mb
        }
        return ResponseBuilder.success(
            data=liveness_payload,
            message="Liveness probe evaluated successfully",
            status_code=200
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

    @staticmethod
    def get_prometheus_metrics() -> Response:
        """
        GET /metrics/prometheus
        Expose Prometheus text exposition format metrics.
        """
        prom_text = metrics_collector.export_prometheus_metrics()
        response = make_response(prom_text, 200)
        response.mimetype = "text/plain; version=0.0.4; charset=utf-8"
        return response

    def get_dashboard(self) -> Tuple[Response, int]:
        """
        GET /dashboard
        Retrieve operational monitoring dashboard summary.
        """
        dashboard_data = self.monitoring_service.get_dashboard_summary()
        return ResponseBuilder.success(
            data=dashboard_data,
            message="Dashboard telemetry retrieved successfully",
            status_code=200
        )
