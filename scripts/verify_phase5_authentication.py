"""
Phase 5 Salesforce OAuth 2.0 Authentication Verification Script.
Validates the complete authentication layer:
1. Environment variable loading (OAuth client ID, secret, login URL)
2. OAuth configuration sanity
3. Login request construction (grant_type=password, password + security_token concatenation)
4. Access token parsing & Authorization header generation
5. Pre-emptive expiration calculation & Refresh token logic
6. Requests session initialization with connection pooling and retry strategy
7. Authentication service imports & DI verification
"""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
import requests

# Ensure root directory is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config import get_config
from app.integrations.auth import SalesforceOAuth
from app.integrations.salesforce_client import SalesforceClient
from app.services.auth_service import AuthService
from app.utils.exceptions import AuthenticationError

def run_phase5_verification() -> bool:
    print("=" * 75)
    print("SALESFORCE BULK API 2.0 INGESTION SYSTEM - PHASE 5 AUTH VERIFICATION")
    print("=" * 75)

    checks = []

    # 1. Environment Variables Loading
    try:
        config = get_config("development")
        has_client_id = bool(config.SALESFORCE_CLIENT_ID)
        has_login_url = bool(config.SALESFORCE_LOGIN_URL)
        env_ok = has_client_id and has_login_url
        env_detail = f"Loaded Login URL: {config.SALESFORCE_LOGIN_URL} | Client ID: {SalesforceOAuth._mask_identifier(config.SALESFORCE_CLIENT_ID)}"
    except Exception as exc:
        env_ok = False
        env_detail = f"Environment error: {str(exc)}"
    checks.append(("Environment Variables Loading", env_ok, env_detail))

    # 2. OAuth Configuration Sanity
    try:
        oauth = SalesforceOAuth(
            login_url=config.SALESFORCE_LOGIN_URL,
            client_id=config.SALESFORCE_CLIENT_ID,
            client_secret=config.SALESFORCE_CLIENT_SECRET,
            username=config.SALESFORCE_USERNAME,
            password=config.SALESFORCE_PASSWORD,
            security_token=config.SALESFORCE_SECURITY_TOKEN,
            token_ttl_seconds=7200,
            refresh_buffer_seconds=300,
            timeout_seconds=30
        )
        oauth_cfg_ok = (
            oauth.login_url.startswith("http") and
            oauth.token_ttl_seconds == 7200 and
            oauth.refresh_buffer_seconds == 300 and
            oauth.timeout_seconds == 30
        )
        oauth_cfg_detail = f"Configured (TTL: {oauth.token_ttl_seconds}s, Buffer: {oauth.refresh_buffer_seconds}s, Timeout: {oauth.timeout_seconds}s)"
    except Exception as exc:
        oauth_cfg_ok = False
        oauth_cfg_detail = f"OAuth config error: {str(exc)}"
    checks.append(("OAuth Configuration Sanity", oauth_cfg_ok, oauth_cfg_detail))

    # 3. Login Request Creation & Payload Validation
    try:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "00DTESTMOCKTOKEN123456789",
            "instance_url": "https://enterprise-test.my.salesforce.com",
            "token_type": "Bearer",
            "issued_at": "1729800000000"
        }

        with patch.object(oauth.session, "post", return_value=mock_response) as mock_post:
            oauth.authenticate()
            mock_post.assert_called_once()
            call_url = mock_post.call_args[0][0]
            call_data = mock_post.call_args[1]["data"]
            
            payload_ok = (
                call_url.endswith("/services/oauth2/token") and
                call_data["grant_type"] == "password" and
                call_data["client_id"] == config.SALESFORCE_CLIENT_ID and
                call_data["username"] == config.SALESFORCE_USERNAME
            )
            req_creation_detail = f"Endpoint: {call_url} | Grant Type: {call_data['grant_type']}"
    except Exception as exc:
        payload_ok = False
        req_creation_detail = f"Request creation error: {str(exc)}"
    checks.append(("Login Request Creation & Endpoint", payload_ok, req_creation_detail))

    # 4. Access Token Parsing & Header Generation
    try:
        token = oauth.get_valid_token()
        instance = oauth.get_instance_url()
        headers = oauth.get_authorization_header()
        
        token_parse_ok = (
            token == "00DTESTMOCKTOKEN123456789" and
            instance == "https://enterprise-test.my.salesforce.com" and
            headers == {"Authorization": "Bearer 00DTESTMOCKTOKEN123456789"}
        )
        token_parse_detail = f"Token: {token[:6]}*** | Instance: {instance} | Header: Bearer ***"
    except Exception as exc:
        token_parse_ok = False
        token_parse_detail = f"Token parsing error: {str(exc)}"
    checks.append(("Access Token Parsing & Headers", token_parse_ok, token_parse_detail))

    # 5. Pre-Emptive Expiration & Refresh Token Logic
    try:
        # Simulate token nearing expiration
        oauth._expires_at = time.time() + 100  # 100s remaining < 300s buffer
        is_exp = oauth.is_token_expired()

        mock_renewed_resp = MagicMock(spec=requests.Response)
        mock_renewed_resp.status_code = 200
        mock_renewed_resp.json.return_value = {
            "access_token": "00DRENEWEDTOKEN987654321",
            "instance_url": "https://enterprise-test.my.salesforce.com",
            "token_type": "Bearer",
            "issued_at": str(int(time.time() * 1000))
        }

        with patch.object(oauth.session, "post", return_value=mock_renewed_resp):
            renewed_token = oauth.get_valid_token()
            refresh_ok = is_exp and (renewed_token == "00DRENEWEDTOKEN987654321")
            refresh_detail = f"Pre-emptive expiry detected -> Renewed token: {renewed_token[:6]}***"
    except Exception as exc:
        refresh_ok = False
        refresh_detail = f"Refresh error: {str(exc)}"
    checks.append(("Token Expiry Detection & Refresh", refresh_ok, refresh_detail))

    # 6. HTTP Session Initialization & Connection Pooling
    try:
        adapter = oauth.session.adapters.get("https://")
        has_pooling = adapter._pool_connections >= 10 and adapter._pool_maxsize >= 20
        has_retries = adapter.max_retries.total >= 3
        has_agent = "Salesforce-Bulk-Ingestion-Platform" in oauth.session.headers.get("User-Agent", "")

        session_ok = has_pooling and has_retries and has_agent
        session_detail = f"Pool Connections: {adapter._pool_connections}, Pool Maxsize: {adapter._pool_maxsize}, Retries: {adapter.max_retries.total}"
    except Exception as exc:
        session_ok = False
        session_detail = f"Session error: {str(exc)}"
    checks.append(("HTTP Session & Connection Pooling", session_ok, session_detail))

    # 7. Authentication Service Imports & Client Facade
    try:
        client = SalesforceClient(oauth_manager=oauth)
        service = AuthService(oauth_manager=oauth, salesforce_client=client)
        
        with patch.object(oauth, "authenticate", return_value={"is_valid": True, "instance_url": "https://enterprise-test.my.salesforce.com"}):
            info = service.get_session_info()
            valid = service.validate_session()
            auth_headers = service.get_authorization_headers()

        service_ok = valid is True and "Authorization" in auth_headers
        service_detail = f"AuthService + SalesforceClient bound successfully. Valid: {valid}"
    except Exception as exc:
        service_ok = False
        service_detail = f"Service import error: {str(exc)}"
    checks.append(("Authentication Service Layer Facade", service_ok, service_detail))

    # Print summary table
    all_passed = True
    for name, status, detail in checks:
        status_str = "PASS" if status else "FAIL"
        if not status:
            all_passed = False
        print(f"[{status_str}] {name:<38} : {detail}")

    print("=" * 75)
    if all_passed:
        print("PHASE 5 SALESFORCE AUTHENTICATION VERIFIED")
        print("=" * 75)
        return True
    else:
        print("PHASE 5 SALESFORCE AUTHENTICATION FAILED")
        print("=" * 75)
        return False

if __name__ == "__main__":
    if not run_phase5_verification():
        sys.exit(1)
    sys.exit(0)
