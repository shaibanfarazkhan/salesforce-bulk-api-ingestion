"""
Salesforce OAuth 2.0 Authentication Manager.
Implements the OAuth 2.0 Username-Password & Security Token flow with
thread-safe mutex token caching, pre-emptive expiration tracking,
resilient HTTP connection pooling, retry adapters, and sensitive credential masking.
"""

import time
import threading
import logging
from typing import Dict, Any, Optional
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.utils.exceptions import AuthenticationError, SalesforceAPIError

logger = logging.getLogger(__name__)

class SalesforceOAuth:
    """
    Manages OAuth 2.0 token lifecycle for Salesforce multi-tenant APIs.
    
    Attributes:
        login_url (str): Salesforce Identity authorization base URL (e.g. https://login.salesforce.com).
        client_id (str): Connected App OAuth 2.0 Client ID.
        client_secret (str): Connected App OAuth 2.0 Client Secret.
        username (str): Integration user username.
        password (str): Integration user password.
        security_token (str): User security token appended to password.
    """

    def __init__(
        self,
        login_url: str = "https://login.salesforce.com",
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        security_token: Optional[str] = None,
        token_ttl_seconds: int = 7200,
        refresh_buffer_seconds: int = 300,
        timeout_seconds: int = 30
    ) -> None:
        self.login_url = login_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.security_token = security_token or ""
        self.token_ttl_seconds = token_ttl_seconds
        self.refresh_buffer_seconds = refresh_buffer_seconds
        self.timeout_seconds = timeout_seconds

        # Internal Token Cache
        self._access_token: Optional[str] = None
        self._instance_url: Optional[str] = None
        self._token_type: str = "Bearer"
        self._expires_at: float = 0.0
        self._issued_at: Optional[str] = None

        # Thread synchronization mutex for multi-worker environments
        self._lock = threading.Lock()

        # Initialize dedicated resilient requests session
        self.session = self._create_resilient_session()

    def _create_resilient_session(self) -> requests.Session:
        """
        Create a requests.Session configured with HTTP connection pooling,
        exponential backoff retries on transient errors, and strict headers.
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET"]
        )
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=retry_strategy
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "User-Agent": "Salesforce-Bulk-Ingestion-Platform/1.0",
            "Accept": "application/json"
        })
        return session

    def is_token_expired(self) -> bool:
        """
        Check if the cached token is missing or near expiration (within buffer window).
        """
        if not self._access_token or self._expires_at <= 0:
            return True
        return time.time() >= (self._expires_at - self.refresh_buffer_seconds)

    def authenticate(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Acquire or refresh Salesforce OAuth 2.0 access token via Username-Password flow.
        Guarded by a thread lock to prevent simultaneous refresh stampedes.
        
        Args:
            force_refresh: If True, bypass cache and request fresh token.
            
        Returns:
            dict: Parsed OAuth token payload with access_token and instance_url.
            
        Raises:
            AuthenticationError: On invalid credentials or unauthorized client.
            SalesforceAPIError: On upstream server or network timeout failures.
        """
        with self._lock:
            # If another thread already refreshed while waiting on the lock, return cached
            if not force_refresh and not self.is_token_expired():
                return self.get_session_info()

            token_endpoint = f"{self.login_url}/services/oauth2/token"
            full_password = f"{self.password}{self.security_token}" if self.password else ""

            payload = {
                "grant_type": "password",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "username": self.username,
                "password": full_password
            }

            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }

            logger.info(
                f"Initiating Salesforce OAuth 2.0 handshake to {self.login_url} "
                f"for user: {self._mask_identifier(self.username or '')}"
            )

            try:
                response = self.session.post(
                    token_endpoint,
                    data=payload,
                    headers=headers,
                    timeout=self.timeout_seconds
                )
            except requests.exceptions.Timeout as exc:
                logger.error(f"Salesforce OAuth token request timed out after {self.timeout_seconds}s.")
                raise SalesforceAPIError("Salesforce OAuth token request timed out", status_code=504) from exc
            except requests.exceptions.RequestException as exc:
                logger.error(f"Network failure connecting to Salesforce OAuth endpoint: {str(exc)}")
                raise SalesforceAPIError(f"Network error during Salesforce authentication: {str(exc)}", status_code=502) from exc

            if response.status_code != 200:
                self._handle_auth_error(response)

            token_data = response.json()
            self._access_token = token_data.get("access_token")
            self._instance_url = token_data.get("instance_url", "").rstrip("/")
            self._token_type = token_data.get("token_type", "Bearer")
            self._issued_at = token_data.get("issued_at")
            self._expires_at = time.time() + self.token_ttl_seconds

            logger.info(
                f"Salesforce OAuth 2.0 authentication successful. "
                f"Instance: {self._instance_url} | Expires in: {self.token_ttl_seconds}s"
            )

            return self.get_session_info()

    def get_valid_token(self) -> str:
        """
        Retrieve a guaranteed valid access token, auto-refreshing if expired.
        """
        if self.is_token_expired():
            self.authenticate()
        return self._access_token or ""

    def get_instance_url(self) -> str:
        """
        Retrieve the verified Salesforce instance URL, authenticating if not present.
        """
        if not self._instance_url or self.is_token_expired():
            self.authenticate()
        return self._instance_url or ""

    def get_authorization_header(self) -> Dict[str, str]:
        """
        Generate HTTP Authorization header dictionary for upstream requests.
        """
        token = self.get_valid_token()
        return {"Authorization": f"{self._token_type} {token}"}

    def get_session_info(self) -> Dict[str, Any]:
        """
        Return non-sensitive active session metadata.
        """
        return {
            "instance_url": self._instance_url,
            "token_type": self._token_type,
            "expires_in": max(0, int(self._expires_at - time.time())) if self._expires_at else 0,
            "issued_at": self._issued_at,
            "is_valid": not self.is_token_expired()
        }

    def _handle_auth_error(self, response: requests.Response) -> None:
        """
        Parse and translate Salesforce OAuth error codes into typed domain exceptions.
        """
        try:
            err_json = response.json()
            error = err_json.get("error", "unknown_error")
            error_description = err_json.get("error_description", response.text)
        except Exception:
            error = "unknown_error"
            error_description = response.text

        logger.warning(
            f"Salesforce OAuth error [{response.status_code}]: {error} - {error_description}"
        )

        if response.status_code in (400, 401):
            if error in ("invalid_grant", "authentication_failure"):
                raise AuthenticationError(
                    message=f"Salesforce authentication failed: {error_description}",
                    details={"error": error, "status_code": response.status_code}
                )
            if error in ("invalid_client", "invalid_client_credentials"):
                raise AuthenticationError(
                    message="Invalid Salesforce Connected App Client ID or Secret",
                    details={"error": error, "status_code": response.status_code}
                )
        raise SalesforceAPIError(
            message=f"Salesforce OAuth service error: {error_description}",
            status_code=response.status_code,
            details={"error": error}
        )

    @staticmethod
    def _mask_identifier(ident: str) -> str:
        """Mask username or ID for privacy compliance."""
        if not ident or len(ident) <= 4:
            return "***"
        return f"{ident[:2]}***{ident[-2:]}"
