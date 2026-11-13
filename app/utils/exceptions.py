"""
Custom Enterprise Exception Hierarchy.
Provides typed exceptions for all application, integration, and persistence errors
with direct mapping to HTTP status codes and RFC 7807 Problem Details.
"""

from typing import Any, Dict, Optional

class AppException(Exception):
    """
    Base application exception.
    
    Attributes:
        message (str): Human-readable error description.
        status_code (int): Corresponding HTTP status code.
        error_code (str): Machine-readable classification code.
        details (dict): Optional contextual diagnostics.
    """
    def __init__(
        self,
        message: str = "An unexpected error occurred",
        status_code: int = 500,
        error_code: str = "INTERNAL_SERVER_ERROR",
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception attributes to an RFC 7807 compliant dictionary."""
        return {
            "title": self.error_code,
            "status": self.status_code,
            "detail": self.message,
            "context": self.details
        }

class ValidationError(AppException):
    """Raised when client input fails schema validation."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=400,
            error_code="VALIDATION_ERROR",
            details=details
        )

class AuthenticationError(AppException):
    """Raised when credentials or tokens are invalid or expired."""
    def __init__(self, message: str = "Authentication failed", details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=401,
            error_code="AUTHENTICATION_FAILED",
            details=details
        )

class SalesforceAPIError(AppException):
    """Raised when upstream Salesforce REST/Bulk API returns an error status."""
    def __init__(
        self,
        message: str,
        status_code: int = 502,
        error_code: str = "SALESFORCE_API_ERROR",
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(
            message=message,
            status_code=status_code,
            error_code=error_code,
            details=details
        )

class RateLimitExceededError(SalesforceAPIError):
    """Raised when Salesforce Governor Limits (e.g. 10k daily Bulk jobs) are reached."""
    def __init__(self, message: str = "Salesforce API limit exceeded", details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=429,
            error_code="RATE_LIMIT_EXCEEDED",
            details=details
        )

class DatabaseError(AppException):
    """Raised when persistence operations fail."""
    def __init__(self, message: str = "Database operation failed", details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=500,
            error_code="DATABASE_ERROR",
            details=details
        )

class ResourceNotFoundError(AppException):
    """Raised when requested entity is not found."""
    def __init__(self, message: str = "Resource not found", details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=404,
            error_code="RESOURCE_NOT_FOUND",
            details=details
        )

# ==============================================================================
# Phase 6: Bulk API 2.0 Integration Exceptions
# ==============================================================================
class BulkJobException(AppException):
    """Raised when Salesforce Bulk API 2.0 job creation or state transition fails."""
    def __init__(self, message: str, status_code: int = 502, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=status_code,
            error_code="BULK_JOB_ERROR",
            details=details
        )

class InvalidSOQLError(BulkJobException):
    """Raised when an invalid SOQL query string is provided for Bulk API submission."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=400,
            details=details
        )
        self.error_code = "INVALID_SOQL"

class PollingException(AppException):
    """Raised when Bulk API job status polling times out or transitions to a failed state."""
    def __init__(self, message: str, status_code: int = 504, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=status_code,
            error_code="POLLING_ERROR",
            details=details
        )

class BulkQueryTimeoutError(PollingException):
    """Raised when polling query results exceeds maximum allowed duration."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=504,
            details=details
        )
        self.error_code = "QUERY_TIMEOUT"

class DownloadException(AppException):
    """Raised when CSV stream downloading from Salesforce storage fails."""
    def __init__(self, message: str, status_code: int = 502, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=status_code,
            error_code="DOWNLOAD_ERROR",
            details=details
        )

class CSVException(AppException):
    """Raised when parsing or processing CSV stream chunks fails."""
    def __init__(self, message: str, status_code: int = 422, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=status_code,
            error_code="CSV_PROCESSING_ERROR",
            details=details
        )

class ValidationException(AppException):
    """Raised when record data validation constraints fail."""
    def __init__(self, message: str, status_code: int = 422, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=status_code,
            error_code="VALIDATION_ERROR",
            details=details
        )

class RepositoryException(AppException):
    """Raised when repository upsert or database persistence fails."""
    def __init__(self, message: str, status_code: int = 500, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=status_code,
            error_code="REPOSITORY_ERROR",
            details=details
        )

# ==============================================================================
# Phase 7: Enterprise Processing & Resilience Exceptions
# ==============================================================================
class CircuitBreakerOpenException(AppException):
    """Raised when an operation is blocked because the circuit breaker is in OPEN state."""
    def __init__(self, message: str = "Circuit breaker is OPEN. Fast-failing request.", details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=503,
            error_code="CIRCUIT_BREAKER_OPEN",
            details=details
        )

class QueueFullException(AppException):
    """Raised when the worker processing queue reaches maximum capacity."""
    def __init__(self, message: str = "Worker queue capacity reached. Cannot accept new jobs.", details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=429,
            error_code="QUEUE_FULL",
            details=details
        )

class DeadLetterException(AppException):
    """Raised when a job exhausts all retries or experiences fatal error and is sent to DLQ."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=500,
            error_code="DEAD_LETTER_JOB",
            details=details
        )

class SchedulerException(AppException):
    """Raised when background job scheduler encounters a configuration or execution error."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message=message,
            status_code=500,
            error_code="SCHEDULER_ERROR",
            details=details
        )

