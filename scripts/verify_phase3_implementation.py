"""
Phase 3 Implementation Verification Script.
Validates the complete Clean Architecture folder structure, imports,
blueprint registrations, database initialization, environment loading,
logging configuration, and health endpoints.
"""

import sys
import os
from pathlib import Path

# Ensure root directory is in sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

def run_phase3_verification() -> bool:
    print("=" * 75)
    print("SALESFORCE BULK API 2.0 INGESTION SYSTEM - PHASE 3 VERIFICATION")
    print("=" * 75)

    checks = []

    # 1. Folder & File Structure Verification
    required_paths = [
        "app/__init__.py",
        "app/config.py",
        "app/api/routes.py",
        "app/controllers/auth_controller.py",
        "app/controllers/bulk_controller.py",
        "app/controllers/health_controller.py",
        "app/services/auth_service.py",
        "app/services/bulk_service.py",
        "app/services/polling_service.py",
        "app/services/csv_stream_service.py",
        "app/services/validation_service.py",
        "app/repositories/bulk_repository.py",
        "app/database/database.py",
        "app/database/models.py",
        "app/schemas/auth_schema.py",
        "app/schemas/bulk_schema.py",
        "app/middleware/error_handler.py",
        "app/middleware/logging.py",
        "app/utils/constants.py",
        "app/utils/exceptions.py",
        "app/utils/helpers.py",
        "app/integrations/salesforce_client.py",
        "migrations",
        "tests/conftest.py",
        "tests/test_health.py",
        "docs",
        "scripts",
        "run.py",
        "requirements.txt",
        "Dockerfile",
        "docker-compose.yml",
        ".env.example",
        "README.md"
    ]

    missing_paths = [p for p in required_paths if not (ROOT_DIR / p).exists()]
    structure_ok = len(missing_paths) == 0
    checks.append((
        "Folder & File Structure",
        structure_ok,
        f"Verified all {len(required_paths)} core architecture paths" if structure_ok else f"Missing: {missing_paths}"
    ))

    # 2. Module Imports Verification
    try:
        from app.config import get_config, BaseConfig
        from app.database.database import db, init_db, check_db_health
        from app.database.models import (
            IngestionJob, IngestionError, SalesforceAccount, SalesforceContact, SalesforceOpportunity
        )
        from app.repositories.bulk_repository import BulkRepository
        from app.services.auth_service import AuthService
        from app.services.bulk_service import BulkService
        from app.controllers.health_controller import HealthController
        from app.integrations.salesforce_client import SalesforceClient
        from app.api.routes import api_bp
        from app import create_app
        imports_ok = True
        import_detail = "All Clean Architecture layers and modules imported without error"
    except Exception as exc:
        imports_ok = False
        import_detail = f"Import error: {str(exc)}"
    checks.append(("Module Imports", imports_ok, import_detail))

    # 3. Environment & Configuration Loading
    try:
        config = get_config("development")
        env_vars_present = bool(
            config.DB_HOST and config.DB_PORT and config.DB_NAME and 
            config.SALESFORCE_CLIENT_ID and config.SALESFORCE_LOGIN_URL
        )
        config_ok = env_vars_present and config.CHUNK_SIZE > 0
        config_detail = f"Loaded Config (DB: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}, Chunk: {config.CHUNK_SIZE})"
    except Exception as exc:
        config_ok = False
        config_detail = f"Config error: {str(exc)}"
    checks.append(("Environment Configuration Loading", config_ok, config_detail))

    # 4. Application Factory & Blueprint Registration
    app = None
    try:
        app = create_app("testing")
        blueprints = list(app.blueprints.keys())
        has_api_bp = "api" in blueprints
        
        # Verify routes
        rule_map = [r.rule for r in app.url_map.iter_rules()]
        expected_endpoints = ["/health", "/metrics", "/login", "/refresh", "/bulk/jobs", "/ingest", "/logs"]
        all_routes_mapped = all(any(ep in r for r in rule_map) for ep in expected_endpoints)
        
        factory_ok = has_api_bp and all_routes_mapped
        factory_detail = f"Blueprint 'api' registered with {len(rule_map)} active routes"
    except Exception as exc:
        factory_ok = False
        factory_detail = f"Factory error: {str(exc)}"
    checks.append(("Blueprint Registration & URL Mapping", factory_ok, factory_detail))

    # 5. Database Initialization
    try:
        if app is None:
            raise RuntimeError("App instance was not initialized")
        with app.app_context():
            db.create_all()
            db_status = check_db_health()
            db_init_ok = db_status.get("status") == "UP"
            db_detail = f"SQLAlchemy engine connected. Dialect: {db_status.get('dialect')}"
    except Exception as exc:
        db_init_ok = False
        db_detail = f"Database error: {str(exc)}"
    checks.append(("Database Initialization & Health Probe", db_init_ok, db_detail))

    # 6. Enterprise Logging Verification
    try:
        import logging
        log_dir = Path(app.config.get("LOG_DIR", "logs"))
        log_file = log_dir / "app.log"
        app.logger.warning("Phase 3 verification test log entry.")
        for h in list(app.logger.handlers) + list(logging.getLogger().handlers):
            h.flush()
        logging_ok = log_file.exists() and log_file.stat().st_size > 0
        log_detail = f"Rotating file logging active at {log_file} ({log_file.stat().st_size} bytes)"
    except Exception as exc:
        logging_ok = False
        log_detail = f"Logging error: {str(exc)}"
    checks.append(("Enterprise Logging & Rotating Handlers", logging_ok, log_detail))

    # 7. Health Endpoint Execution
    try:
        client = app.test_client()
        response = client.get("/health")
        resp_json = response.get_json()
        health_endpoint_ok = (
            response.status_code == 200 and 
            resp_json.get("status") == "SUCCESS" and 
            "services" in resp_json.get("data", {})
        )
        health_detail = f"GET /health responded 200 OK (Status: {resp_json.get('data', {}).get('status')})"
    except Exception as exc:
        health_endpoint_ok = False
        health_detail = f"Health endpoint error: {str(exc)}"
    checks.append(("Health Endpoint & Correlation Tracing", health_endpoint_ok, health_detail))

    # Print summary table
    all_passed = True
    for name, status, detail in checks:
        status_str = "PASS" if status else "FAIL"
        if not status:
            all_passed = False
        print(f"[{status_str}] {name:<38} : {detail}")

    print("=" * 75)
    if all_passed:
        print("PHASE 3 IMPLEMENTATION VERIFIED")
        print("=" * 75)
        return True
    else:
        print("PHASE 3 IMPLEMENTATION FAILED")
        print("=" * 75)
        return False

if __name__ == "__main__":
    if not run_phase3_verification():
        sys.exit(1)
    sys.exit(0)
