"""
Enterprise Helper Utilities and Response Builders.
Provides standardized JSON response formatting, correlation ID handling,
and schema validation utility wrappers.
"""

from typing import Any, Dict, Optional, Tuple
from flask import jsonify, Response, g, request
from app.utils.constants import HEADER_CORRELATION_ID

class ResponseBuilder:
    """
    Standardized Enterprise API Response Builder.
    Encapsulates uniform JSON payload construction for success and error scenarios.
    """

    @staticmethod
    def success(
        data: Optional[Any] = None,
        message: str = "Operation completed successfully",
        status_code: int = 200,
        meta: Optional[Dict[str, Any]] = None
    ) -> Tuple[Response, int]:
        """
        Build a standardized success JSON response.
        
        Args:
            data: Main payload returned to client.
            message: Informational human-readable message.
            status_code: HTTP status code (200, 201, 202).
            meta: Optional pagination or execution telemetry metadata.
        """
        correlation_id = getattr(g, "correlation_id", request.headers.get(HEADER_CORRELATION_ID, ""))
        payload: Dict[str, Any] = {
            "status": "SUCCESS",
            "correlation_id": correlation_id,
            "message": message,
            "data": data if data is not None else {}
        }
        if meta:
            payload["meta"] = meta
            
        resp = jsonify(payload)
        resp.headers[HEADER_CORRELATION_ID] = correlation_id
        return resp, status_code

    @staticmethod
    def error(
        title: str,
        detail: str,
        status_code: int = 500,
        error_code: str = "INTERNAL_SERVER_ERROR",
        context: Optional[Dict[str, Any]] = None
    ) -> Tuple[Response, int]:
        """
        Build an RFC 7807 Problem Details compliant error JSON response.
        """
        correlation_id = getattr(g, "correlation_id", request.headers.get(HEADER_CORRELATION_ID, ""))
        problem: Dict[str, Any] = {
            "type": f"https://api.enterprise.com/errors/{error_code.lower()}",
            "title": title,
            "status": status_code,
            "detail": detail,
            "instance": request.path if request else "",
            "correlation_id": correlation_id
        }
        if context:
            problem["context"] = context

        resp = jsonify(problem)
        resp.status_code = status_code
        resp.content_type = "application/problem+json"
        resp.headers[HEADER_CORRELATION_ID] = correlation_id
        return resp, status_code
