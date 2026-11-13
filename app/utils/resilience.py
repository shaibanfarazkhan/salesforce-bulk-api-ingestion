"""
Enterprise Resilience Utilities.
Provides transient error classification, circuit breaker state machine,
and decorrelated jitter backoff calculation for Bulk API ingestion pipelines.
"""

import time
import random
import threading
import logging
from typing import Callable, Any, Optional, Dict
import requests

from app.utils.constants import CircuitState
from app.utils.exceptions import (
    CircuitBreakerOpenException,
    InvalidSOQLError,
    ValidationError,
    ResourceNotFoundError,
    RateLimitExceededError,
    SalesforceAPIError,
    PollingException,
    DownloadException,
    BulkQueryTimeoutError
)

logger = logging.getLogger(__name__)

def is_retryable_error(exc: Exception) -> bool:
    """
    Classify an exception as retryable (transient) or non-retryable (fatal).
    
    Retryable:
        - Network connectivity/socket errors
        - HTTP 502 Bad Gateway, 503 Service Unavailable, 504 Gateway Timeout
        - HTTP 408 Request Timeout
        - Polling timeouts or transient Salesforce query lags
        - Temporary rate limit throttles (with backoff)
        
    Non-retryable:
        - Invalid SOQL syntax errors (400)
        - Schema / Record validation errors
        - Entity / Resource Not Found (404)
        - Invalid Credentials / Authentication failures
    """
    if isinstance(exc, (InvalidSOQLError, ValidationError, ResourceNotFoundError)):
        return False

    if isinstance(exc, requests.exceptions.RequestException):
        # Network drops, DNS lookup failures, connection resets
        if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
            return True
        if hasattr(exc, "response") and exc.response is not None:
            return exc.response.status_code in (408, 429, 500, 502, 503, 504)
        return True

    if isinstance(exc, (BulkQueryTimeoutError, PollingException, DownloadException)):
        status_code = getattr(exc, "status_code", 500)
        return status_code in (408, 429, 500, 502, 503, 504)

    if isinstance(exc, SalesforceAPIError):
        status = getattr(exc, "status_code", 500)
        return status in (408, 429, 500, 502, 503, 504)

    # General OSError / TimeoutError
    if isinstance(exc, (TimeoutError, ConnectionResetError, BrokenPipeError)):
        return True

    return False

def compute_decorrelated_jitter(
    base_delay: float = 2.0,
    current_delay: float = 2.0,
    max_delay: float = 60.0
) -> float:
    """
    Calculate sleep interval using Decorrelated Exponential Jitter algorithm.
    Formula: sleep = min(max_delay, Uniform(base_delay, current_delay * 3))
    """
    sleep_val = min(
        max_delay,
        random.uniform(base_delay, max(base_delay, current_delay * 3.0))
    )
    return round(sleep_val, 2)

class CircuitBreaker:
    """
    Thread-safe Circuit Breaker pattern implementation.
    Transitions:
        - CLOSED -> OPEN when consecutive failures >= failure_threshold
        - OPEN -> HALF_OPEN when recovery_timeout seconds have elapsed
        - HALF_OPEN -> CLOSED on successful trial execution
        - HALF_OPEN -> OPEN on failed trial execution
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        name: str = "Salesforce-CircuitBreaker"
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    logger.info(f"[{self.name}] Recovery timeout reached -> Transitioning to HALF_OPEN")
            return self._state

    def record_success(self) -> None:
        """Record successful execution and reset circuit to CLOSED state."""
        with self._lock:
            if self._state in (CircuitState.HALF_OPEN, CircuitState.OPEN):
                logger.info(f"[{self.name}] Successful trial execution -> Circuit reset to CLOSED")
            self._failure_count = 0
            self._state = CircuitState.CLOSED

    def record_failure(self, error: Optional[Exception] = None) -> None:
        """Record failed execution and trip circuit to OPEN if threshold exceeded."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            logger.warning(
                f"[{self.name}] Recorded failure ({self._failure_count}/{self.failure_threshold}): {error}"
            )
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                logger.error(
                    f"[{self.name}] Failure threshold ({self.failure_threshold}) reached! "
                    f"Circuit tripped to OPEN for {self.recovery_timeout}s."
                )

    def can_execute(self) -> bool:
        """Check if request is permitted through the circuit breaker."""
        current_st = self.state
        return current_st in (CircuitState.CLOSED, CircuitState.HALF_OPEN)

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute a function protected by the circuit breaker."""
        if not self.can_execute():
            raise CircuitBreakerOpenException(
                message=f"Circuit breaker '{self.name}' is OPEN. Fast-failing downstream request.",
                details={
                    "circuit": self.name,
                    "failure_count": self._failure_count,
                    "recovery_timeout": self.recovery_timeout
                }
            )

        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as exc:
            if is_retryable_error(exc):
                self.record_failure(exc)
            raise

    def get_status(self) -> Dict[str, Any]:
        """Return diagnostic state of the circuit breaker."""
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout_seconds": self.recovery_timeout,
                "last_failure_time": self._last_failure_time
            }

