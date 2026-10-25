"""
Health and Core Skeleton Endpoint Tests.
Validates that Phase 3 endpoints respond with expected JSON contracts and HTTP status codes.
"""

from app.utils.constants import HEADER_CORRELATION_ID

def test_health_endpoint(client):
    """Verify GET /health returns 200 and expected service status dictionary."""
    response = client.get("/health")
    assert response.status_code in [200, 503]
    data = response.get_json()
    assert data["status"] == "SUCCESS"
    assert "services" in data["data"]
    assert HEADER_CORRELATION_ID in response.headers

def test_metrics_endpoint(client):
    """Verify GET /metrics returns 200 and telemetry data."""
    response = client.get("/metrics")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "SUCCESS"
    assert "uptime_seconds" in data["data"]

def test_logs_endpoint(client):
    """Verify GET /logs returns 200 and log payload."""
    response = client.get("/logs")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "SUCCESS"
    assert "logs" in data["data"]

def test_login_validation_failure(client):
    """Verify POST /login with empty payload raises RFC 7807 400 Validation Error."""
    response = client.post("/login", json={})
    assert response.status_code == 400
    data = response.get_json()
    assert data["title"] == "VALIDATION_ERROR"
    assert data["status"] == 400
    assert "context" in data

def test_login_success_placeholder(client):
    """Verify POST /login with valid payload returns 200."""
    from unittest.mock import patch
    from app.services.auth_service import AuthService

    mock_auth_result = {
        "access_token": "00DXXXXXXXXXXXX!ARsAQMOCK_TOKEN",
        "instance_url": "https://enterprise-test.my.salesforce.com",
        "token_type": "Bearer",
        "expires_in": 7200
    }

    with patch.object(AuthService, "login", return_value=mock_auth_result):
        response = client.post("/login", json={
            "username": "admin@enterprise.com",
            "password": "secret_password"
        })
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "SUCCESS"
        assert "access_token" in data["data"]
