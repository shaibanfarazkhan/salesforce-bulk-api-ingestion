"""
Salesforce Authentication Service (Service Layer).
Provides enterprise business orchestration for OAuth 2.0 credential verification,
token validation, pre-emptive session refresh, and authorization header generation.
"""

import os
import logging
from typing import Dict, Any, Optional
from flask import current_app

from app.integrations.auth import SalesforceOAuth
from app.integrations.salesforce_client import SalesforceClient
from app.utils.exceptions import AuthenticationError

logger = logging.getLogger(__name__)

class AuthService:
    """
    Business service orchestrating Salesforce OAuth 2.0 authentication.
    """

    def __init__(
        self,
        oauth_manager: Optional[SalesforceOAuth] = None,
        salesforce_client: Optional[SalesforceClient] = None
    ) -> None:
        if oauth_manager:
            self.oauth_manager = oauth_manager
        else:
            self.oauth_manager = self._init_oauth_from_config()

        self.salesforce_client = salesforce_client or SalesforceClient(oauth_manager=self.oauth_manager)

    def _init_oauth_from_config(self) -> SalesforceOAuth:
        """
        Initialize SalesforceOAuth using Flask current_app config or fallback to get_config().
        """
        config = {}
        try:
            if current_app:
                config = current_app.config
        except Exception:
            pass

        if not config:
            from app.config import get_config
            cfg = get_config()
            login_url = getattr(cfg, "SALESFORCE_LOGIN_URL", "https://login.salesforce.com")
            client_id = getattr(cfg, "SALESFORCE_CLIENT_ID", "")
            client_secret = getattr(cfg, "SALESFORCE_CLIENT_SECRET", "")
            username = getattr(cfg, "SALESFORCE_USERNAME", "")
            password = getattr(cfg, "SALESFORCE_PASSWORD", "")
            security_token = getattr(cfg, "SALESFORCE_SECURITY_TOKEN", "")
        else:
            login_url = config.get("SALESFORCE_LOGIN_URL") or os.getenv("LOGIN_URL", "https://login.salesforce.com")
            client_id = config.get("SALESFORCE_CLIENT_ID") or os.getenv("CLIENT_ID", "")
            client_secret = config.get("SALESFORCE_CLIENT_SECRET") or os.getenv("CLIENT_SECRET", "")
            username = config.get("SALESFORCE_USERNAME") or os.getenv("SF_USERNAME", "")
            password = config.get("SALESFORCE_PASSWORD") or os.getenv("SF_PASSWORD", "")
            security_token = config.get("SALESFORCE_SECURITY_TOKEN") or os.getenv("SF_SECURITY_TOKEN", "")

        return SalesforceOAuth(
            login_url=login_url,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            security_token=security_token
        )

    def login(self, credentials: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Authenticate with Salesforce using provided credentials or configured service account.
        
        Args:
            credentials: Optional override credentials dictionary.
            
        Returns:
            dict: Active session descriptor with access_token and instance_url.
        """
        if credentials:
            # Override credentials for this login request
            if "login_url" in credentials:
                self.oauth_manager.login_url = credentials["login_url"]
            if "client_id" in credentials:
                self.oauth_manager.client_id = credentials["client_id"]
            if "client_secret" in credentials:
                self.oauth_manager.client_secret = credentials["client_secret"]
            if "username" in credentials:
                self.oauth_manager.username = credentials["username"]
            if "password" in credentials:
                self.oauth_manager.password = credentials["password"]
            if "security_token" in credentials:
                self.oauth_manager.security_token = credentials["security_token"]

        return self.oauth_manager.authenticate(force_refresh=True)

    def refresh_token(self) -> Dict[str, Any]:
        """
        Force renewal of current Salesforce OAuth 2.0 access token.
        """
        logger.info("Service layer requested manual OAuth token refresh.")
        return self.oauth_manager.authenticate(force_refresh=True)

    def validate_session(self) -> bool:
        """
        Validate whether the current access token is present and unexpired.
        """
        return not self.oauth_manager.is_token_expired()

    def get_authorization_headers(self) -> Dict[str, str]:
        """
        Retrieve valid HTTP Authorization headers for upstream API requests.
        """
        return self.oauth_manager.get_authorization_header()

    def get_session_info(self) -> Dict[str, Any]:
        """
        Retrieve non-sensitive metadata for the active OAuth session.
        """
        return self.oauth_manager.get_session_info()
