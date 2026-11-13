"""
Application Constants and Enumerations.
Defines system-wide constants, Salesforce Bulk API states, HTTP status codes,
and default timeouts conforming to enterprise standards.
"""

from enum import Enum

class SalesforceJobState(str, Enum):
    """Salesforce Bulk API 2.0 Query Job States."""
    OPEN = "Open"
    UPLOAD_COMPLETE = "UploadComplete"
    IN_PROGRESS = "InProgress"
    JOB_COMPLETE = "JobComplete"
    ABORTED = "Aborted"
    FAILED = "Failed"

class IngestionStatus(str, Enum):
    """Internal Ingestion Workflow States."""
    SUBMITTED = "SUBMITTED"
    IN_PROGRESS = "IN_PROGRESS"
    STREAMING = "STREAMING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"

class ErrorCode(str, Enum):
    """Standardized Application Error Codes."""
    VALIDATION_ERROR = "VALIDATION_ERROR"
    AUTH_ERROR = "AUTHENTICATION_FAILED"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    SALESFORCE_API_ERROR = "SALESFORCE_API_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    CIRCUIT_BREAKER_OPEN = "CIRCUIT_BREAKER_OPEN"
    QUEUE_FULL = "QUEUE_FULL"
    DEAD_LETTER_JOB = "DEAD_LETTER_JOB"

class CircuitState(str, Enum):
    """Circuit Breaker States."""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class DLQStatus(str, Enum):
    """Dead Letter Queue Item Statuses."""
    PENDING_REVIEW = "PENDING_REVIEW"
    REPLAYED = "REPLAYED"
    RESOLVED = "RESOLVED"
    DISCARDED = "DISCARDED"

class SchedulerStatus(str, Enum):
    """Scheduler Runtime States."""
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"

# HTTP Header Constants
HEADER_CORRELATION_ID = "X-Correlation-ID"
HEADER_SF_LIMIT_INFO = "Sforce-Limit-Info"
HEADER_LOCATOR = "Sforce-Locator"

# Defaults
DEFAULT_CHUNK_SIZE = 10000
DEFAULT_TIMEOUT_SECONDS = 30
MAX_POLLING_TIMEOUT_SECONDS = 900  # 15 minutes
POLLING_BASE_INTERVAL = 2.0
POLLING_MAX_INTERVAL = 30.0
