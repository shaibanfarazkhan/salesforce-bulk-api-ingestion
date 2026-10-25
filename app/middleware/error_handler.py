"""
Central Enterprise Error Handling Middleware.
Translates domain exceptions and unhandled system errors into
standard RFC 7807 Problem Details JSON responses.
"""

import traceback
from flask import Flask, Request, Response
from werkzeug.exceptions import HTTPException
from app.utils.exceptions import AppException
from app.utils.helpers import ResponseBuilder

def register_error_handlers(app: Flask) -> None:
    """Register application-wide exception handlers on the Flask app instance."""

    @app.errorhandler(AppException)
    def handle_app_exception(error: AppException):
        """Handle custom typed domain exceptions."""
        app.logger.warning(
            f"Domain Exception Handled: [{error.error_code}] {error.message} - Context: {error.details}"
        )
        return ResponseBuilder.error(
            title=error.error_code,
            detail=error.message,
            status_code=error.status_code,
            error_code=error.error_code,
            context=error.details
        )

    @app.errorhandler(HTTPException)
    def handle_http_exception(error: HTTPException):
        """Handle standard Werkzeug HTTP exceptions (404, 405, 400)."""
        app.logger.info(f"HTTP Exception: {error.code} - {error.description}")
        return ResponseBuilder.error(
            title=error.name,
            detail=error.description or "An HTTP protocol error occurred",
            status_code=error.code or 500,
            error_code=f"HTTP_{error.code}"
        )

    @app.errorhandler(Exception)
    def handle_generic_exception(error: Exception):
        """Catch-all for unhandled system exceptions to prevent unformatted crashes."""
        app.logger.error(
            f"Unhandled System Exception: {str(error)}\n{traceback.format_exc()}"
        )
        return ResponseBuilder.error(
            title="Internal Server Error",
            detail="An unexpected internal server error occurred. Please contact system administrators.",
            status_code=500,
            error_code="INTERNAL_SERVER_ERROR"
        )
