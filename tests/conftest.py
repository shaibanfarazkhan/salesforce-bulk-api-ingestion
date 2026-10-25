"""
Pytest Test Suite Configuration and Fixtures.
Provides isolated test application instances and HTTP test clients.
"""

import sys
from pathlib import Path
import pytest

# Ensure root directory is on Python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import create_app
from app.database.database import db

@pytest.fixture(scope="session")
def app():
    """Create and configure an isolated Flask application instance for testing."""
    test_app = create_app("testing")
    with test_app.app_context():
        db.create_all()
        yield test_app
        db.drop_all()

@pytest.fixture(scope="function")
def client(app):
    """Provide a test HTTP client for issuing requests against the application."""
    return app.test_client()

@pytest.fixture(scope="function")
def runner(app):
    """Provide a CLI test runner for testing Click commands."""
    return app.test_cli_runner()
