"""
Unit Tests for Salesforce OAuth 2.0 Authentication & Client.
Tests token acquisition, automatic refresh on HTTP 401, error translation,
pre-emptive expiration detection, and thread-safe session reuse using mocks.
"""

import time
import pytest
from unittest.mock import MagicMock, patch
import requests

from app.integrations.auth import SalesforceOAuth
from app.integrations.salesforce_client import SalesforceClient
from app.services.auth_service import AuthService
from app.utils.exceptions import AuthenticationError, SalesforceAPIError, RateLimitExceededError

@pytest.fixture
def oauth_manager():
    """Create a test SalesforceOAuth instance."""
    return SalesforceOAuth(
        login_url="https://login.salesforce.com",
        client_id="test_client_id_123",
        client_secret="test_client_secret_xyz",
        username="test.user@enterprise.example.com",
        password="test_password",
        security_token="test_security_token",
        token_ttl_seconds=3600,
        refresh_buffer_seconds=300,
        timeout_seconds=5
    )

def test_oauth_success_authentication(oauth_manager):
    """Verify successful OAuth token grant parsing and header generation."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "00DXXXXXXXXXXXX!ARsAQMOCK_TOKEN_VAL",
        "instance_url": "https://enterprise-test.my.salesforce.com",
        "token_type": "Bearer",
        "issued_at": "1729800000000",
        "signature": "mock_signature_hash"
    }

    with patch.object(oauth_manager.session, "post", return_value=mock_response) as mock_post:
        session_info = oauth_manager.authenticate()
        
        # Verify call arguments
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args[1]
        assert call_kwargs["data"]["grant_type"] == "password"
        assert call_kwargs["data"]["username"] == "test.user@enterprise.example.com"
        assert call_kwargs["data"]["password"] == "test_passwordtest_security_token"

        # Verify parsed state
        assert session_info["instance_url"] == "https://enterprise-test.my.salesforce.com"
        assert session_info["is_valid"] is True
        assert oauth_manager.get_valid_token() == "00DXXXXXXXXXXXX!ARsAQMOCK_TOKEN_VAL"
        assert oauth_manager.get_authorization_header() == {
            "Authorization": "Bearer 00DXXXXXXXXXXXX!ARsAQMOCK_TOKEN_VAL"
        }
        assert oauth_manager.is_token_expired() is False

def test_oauth_invalid_credentials_error(oauth_manager):
    """Verify invalid_grant raises typed AuthenticationError."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 400
    mock_response.json.return_value = {
        "error": "invalid_grant",
        "error_description": "authentication failure - user credentials invalid"
    }

    with patch.object(oauth_manager.session, "post", return_value=mock_response):
        with pytest.raises(AuthenticationError) as exc_info:
            oauth_manager.authenticate()
        assert "authentication failure" in str(exc_info.value)
        assert exc_info.value.status_code == 401

def test_oauth_invalid_client_credentials(oauth_manager):
    """Verify invalid_client raises AuthenticationError."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 400
    mock_response.json.return_value = {
        "error": "invalid_client",
        "error_description": "invalid client credentials"
    }

    with patch.object(oauth_manager.session, "post", return_value=mock_response):
        with pytest.raises(AuthenticationError) as exc_info:
            oauth_manager.authenticate()
        assert "Invalid Salesforce Connected App Client ID or Secret" in str(exc_info.value)

def test_oauth_token_expiration_logic(oauth_manager):
    """Verify token expiration calculation and pre-emptive refresh window."""
    assert oauth_manager.is_token_expired() is True

    oauth_manager._access_token = "valid_token"
    oauth_manager._expires_at = time.time() + 1000  # 1000s in the future
    oauth_manager.refresh_buffer_seconds = 300
    assert oauth_manager.is_token_expired() is False

    # Within buffer window (e.g. 200s left < 300s buffer)
    oauth_manager._expires_at = time.time() + 200
    assert oauth_manager.is_token_expired() is True

def test_client_automatic_token_refresh_on_401(oauth_manager):
    """Verify SalesforceClient intercepts HTTP 401, refreshes token, and retries request."""
    client = SalesforceClient(oauth_manager=oauth_manager)

    # First request returns 401, second retry returns 200
    resp_401 = MagicMock(spec=requests.Response)
    resp_401.status_code = 401
    resp_401.headers = {}

    resp_200 = MagicMock(spec=requests.Response)
    resp_200.status_code = 200
    resp_200.headers = {"Sforce-Limit-Info": "api-usage=150/100000"}
    resp_200.json.return_value = {"records": [{"Id": "001300000001AAA001"}]}

    with patch.object(oauth_manager, "authenticate") as mock_auth, \
         patch.object(oauth_manager, "get_instance_url", return_value="https://mock.salesforce.com"), \
         patch.object(oauth_manager, "get_authorization_header", return_value={"Authorization": "Bearer fresh_token"}), \
         patch.object(oauth_manager.session, "request", side_effect=[resp_401, resp_200]) as mock_request:

        response = client.get("/services/data/v62.0/query")
        assert response.status_code == 200
        assert mock_auth.called
        assert mock_request.call_count == 2
        assert client.last_api_usage == "api-usage=150/100000"

def test_client_rate_limiting_429(oauth_manager):
    """Verify HTTP 429 raises RateLimitExceededError."""
    client = SalesforceClient(oauth_manager=oauth_manager)

    resp_429 = MagicMock(spec=requests.Response)
    resp_429.status_code = 429
    resp_429.headers = {"Sforce-Limit-Info": "api-usage=100000/100000"}

    with patch.object(oauth_manager, "get_instance_url", return_value="https://mock.salesforce.com"), \
         patch.object(oauth_manager, "get_authorization_header", return_value={"Authorization": "Bearer token"}), \
         patch.object(oauth_manager.session, "request", return_value=resp_429):

        with pytest.raises(RateLimitExceededError):
            client.get("/services/data/v62.0/jobs/query")

def test_auth_service_integration(oauth_manager):
    """Verify AuthService login, refresh, validation, and session information."""
    service = AuthService(oauth_manager=oauth_manager)

    with patch.object(oauth_manager, "authenticate") as mock_auth:
        mock_auth.return_value = {
            "instance_url": "https://test.salesforce.com",
            "is_valid": True
        }
        res = service.login()
        assert res["instance_url"] == "https://test.salesforce.com"
        assert mock_auth.called

    with patch.object(oauth_manager, "authenticate") as mock_auth:
        service.refresh_token()
        mock_auth.assert_called_with(force_refresh=True)

    with patch.object(oauth_manager, "is_token_expired", return_value=False):
        assert service.validate_session() is True

def test_mask_identifier():
    """Verify identifier masking handles normal and edge case strings."""
    assert SalesforceOAuth._mask_identifier("admin@enterprise.com") == "ad***om"
    assert SalesforceOAuth._mask_identifier("ab") == "***"
    assert SalesforceOAuth._mask_identifier("") == "***"
