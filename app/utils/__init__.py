"""
Utility Package Initialization.
Exports constants, custom exceptions, and response builders.
"""

from app.utils.constants import (
    SalesforceJobState,
    IngestionStatus,
    ErrorCode,
    HEADER_CORRELATION_ID,
    DEFAULT_CHUNK_SIZE
)
from app.utils.exceptions import (
    AppException,
    ValidationError,
    AuthenticationError,
    SalesforceAPIError,
    RateLimitExceededError,
    DatabaseError,
    ResourceNotFoundError,
    BulkJobException,
    PollingException,
    DownloadException,
    CSVException,
    ValidationException,
    RepositoryException
)
from app.utils.helpers import ResponseBuilder

__all__ = [
    "SalesforceJobState",
    "IngestionStatus",
    "ErrorCode",
    "HEADER_CORRELATION_ID",
    "DEFAULT_CHUNK_SIZE",
    "AppException",
    "ValidationError",
    "AuthenticationError",
    "SalesforceAPIError",
    "RateLimitExceededError",
    "DatabaseError",
    "ResourceNotFoundError",
    "BulkJobException",
    "PollingException",
    "DownloadException",
    "CSVException",
    "ValidationException",
    "RepositoryException",
    "ResponseBuilder"
]
