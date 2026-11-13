"""
Enterprise Logging Configuration & Request Interceptor Middleware.
Implements rotating file logging, console streaming, structured log formatting,
and request lifecycle logging with correlation ID tracing.
"""

import os
import json
import time
import uuid
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone
from flask import Flask, request, g, Response, has_app_context
from app.utils.constants import HEADER_CORRELATION_ID

# Custom Formatter including Correlation ID
class CorrelationLogFormatter(logging.Formatter):
    """Formats log records including correlation ID and thread information."""
    def format(self, record: logging.LogRecord) -> str:
        try:
            if has_app_context() and hasattr(g, "correlation_id"):
                record.correlation_id = g.correlation_id
            else:
                record.correlation_id = "SYSTEM"
        except Exception:
            record.correlation_id = "SYSTEM"
        return super().format(record)

class JSONStructuredFormatter(logging.Formatter):
    """
    Enterprise Structured JSON Formatter.
    Serializes log records into standard JSON objects containing timestamp,
    severity level, correlation ID, request ID, logger module, and error context.
    """
    def format(self, record: logging.LogRecord) -> str:
        corr_id = "SYSTEM"
        req_id = "N/A"
        try:
            if has_app_context():
                if hasattr(g, "correlation_id"):
                    corr_id = g.correlation_id
                if hasattr(g, "request_id"):
                    req_id = g.request_id
        except Exception:
            pass

        log_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "correlation_id": getattr(record, "correlation_id", corr_id),
            "request_id": getattr(record, "request_id", req_id),
            "logger": record.name,
            "line": record.lineno,
            "message": record.getMessage()
        }

        if hasattr(record, "duration_ms"):
            log_payload["duration_ms"] = record.duration_ms
        if hasattr(record, "context"):
            log_payload["context"] = record.context

        if record.exc_info:
            log_payload["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else "Exception",
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info)
            }

        return json.dumps(log_payload)

def setup_logging(app: Flask) -> logging.Logger:
    """
    Configure application-wide enterprise logging.
    Installs both a Console StreamHandler and a RotatingFileHandler.
    """
    log_dir = Path(app.config.get("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "app.log"

    log_level_name = app.config.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    log_format = (
        "[%(asctime)s] [%(levelname)s] [%(correlation_id)s] "
        "[%(name)s:%(lineno)d] - %(message)s"
    )
    formatter = CorrelationLogFormatter(fmt=log_format, datefmt="%Y-%m-%d %H:%M:%S")

    # Root Logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Avoid duplicate handlers if setup_logging is called multiple times
    if not any(isinstance(h, RotatingFileHandler) for h in root_logger.handlers):
        # Rotating File Handler (10MB per file, 5 backups)
        file_handler = RotatingFileHandler(
            filename=str(log_file),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        app.logger.addHandler(file_handler)

    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler) for h in root_logger.handlers):
        # Console Stream Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        app.logger.addHandler(console_handler)

    app.logger.setLevel(log_level)
    app.logger.propagate = True
    app.logger.info("Enterprise logging system initialized.")
    return app.logger

def register_request_logging(app: Flask) -> None:
    """
    Register request lifecycle hooks for correlation tracing and execution latency telemetry.
    """
    @app.before_request
    def before_request_hook():
        # Extract or generate Correlation ID
        incoming_corr_id = request.headers.get(HEADER_CORRELATION_ID)
        g.correlation_id = incoming_corr_id if incoming_corr_id else str(uuid.uuid4())
        g.start_time = time.time()
        app.logger.info(
            f"Incoming Request: {request.method} {request.path} from {request.remote_addr}"
        )

    @app.after_request
    def after_request_hook(response: Response) -> Response:
        duration_ms = (time.time() - getattr(g, "start_time", time.time())) * 1000.0
        response.headers[HEADER_CORRELATION_ID] = getattr(g, "correlation_id", "SYSTEM")
        app.logger.info(
            f"Completed Request: {request.method} {request.path} - "
            f"Status: {response.status_code} - Latency: {duration_ms:.2f}ms"
        )
        return response
