"""
Authentication Controller (Controller Layer).
Handles incoming login requests and manual token refresh triggers.
"""

from typing import Tuple
from flask import request, Response
from app.services.auth_service import AuthService
from app.schemas.auth_schema import LoginRequestSchema
from app.utils.helpers import ResponseBuilder
from app.utils.exceptions import ValidationError

class AuthController:
    """
    Controller handling Salesforce OAuth 2.0 authentication endpoints.
    """

    def __init__(self, auth_service: AuthService = None) -> None:
        self.auth_service = auth_service or AuthService()
        self.login_schema = LoginRequestSchema()

    def login(self) -> Tuple[Response, int]:
        """
        POST /login
        Authenticate with Salesforce using provided credentials.
        """
        json_data = request.get_json(silent=True) or {}
        errors = self.login_schema.validate(json_data)
        if errors:
            raise ValidationError(message="Invalid login request payload", details=errors)

        token_data = self.auth_service.login(json_data)
        return ResponseBuilder.success(
            data=token_data,
            message="Salesforce OAuth 2.0 authentication successful (Placeholder)",
            status_code=200
        )

    def refresh(self) -> Tuple[Response, int]:
        """
        POST /refresh
        Force renewal of current OAuth 2.0 session token.
        """
        token_data = self.auth_service.refresh_token()
        return ResponseBuilder.success(
            data=token_data,
            message="Salesforce OAuth 2.0 token refreshed successfully (Placeholder)",
            status_code=200
        )
