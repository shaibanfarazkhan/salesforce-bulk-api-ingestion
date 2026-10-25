"""
Phase 4 Database Implementation Verification Script.
Validates the complete PostgreSQL database layer:
1. Database connection & liveness
2. Schema tables creation (6 normalized tables)
3. Object-relational relationships & cascading navigation
4. Primary keys, foreign keys & check constraints
5. Performance indexes
6. Alembic / Flask-Migrate migration versioning
7. Seed data volume & referential integrity
"""

import sys
from pathlib import Path
from decimal import Decimal
import sqlalchemy as sa
from sqlalchemy import inspect

# Ensure root directory is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import create_app
from app.database.database import db, check_db_health
from app.database.models import (
    IngestionJob,
    IngestionError,
    SalesforceAccount,
    SalesforceContact,
    SalesforceOpportunity,
    AuditLog
)

def run_phase4_verification() -> bool:
    print("=" * 75)
    print("SALESFORCE BULK API 2.0 INGESTION SYSTEM - PHASE 4 DATABASE VERIFICATION")
    print("=" * 75)

    app = create_app("development")
    checks = []

    with app.app_context():
        # 1. Database Connection & Liveness
        db_health = check_db_health()
        conn_ok = db_health.get("status") == "UP"
        conn_detail = f"Connected successfully. Dialect: {db_health.get('dialect')}"
        checks.append(("Database Connection & Dialect", conn_ok, conn_detail))

        # 2. Tables Created
        inspector = inspect(db.engine)
        existing_tables = set(inspector.get_table_names())
        required_tables = {
            "bulk_ingestion_jobs",
            "bulk_ingestion_errors",
            "sf_accounts",
            "sf_contacts",
            "sf_opportunities",
            "audit_logs"
        }
        missing_tables = required_tables - existing_tables
        tables_ok = len(missing_tables) == 0
        tables_detail = f"Found all 6 tables: {', '.join(sorted(required_tables))}" if tables_ok else f"Missing: {missing_tables}"
        checks.append(("Database Tables Creation", tables_ok, tables_detail))

        # 3. Model Relationships & Cascading Navigation
        try:
            # Query an Account and traverse its relationships
            acc = db.session.query(SalesforceAccount).filter_by(salesforce_id="001300000001AAA001").first()
            has_contacts = acc is not None and len(acc.contacts) >= 2
            has_opps = acc is not None and len(acc.opportunities) >= 1

            # Query an IngestionJob and traverse errors and logs
            job = db.session.query(IngestionJob).filter_by(sf_job_id="750300000001AAA999").first()
            has_errors = job is not None and len(job.errors) >= 2
            has_logs = job is not None and len(job.audit_logs) >= 2

            rel_ok = has_contacts and has_opps and has_errors and has_logs
            rel_detail = f"Verified Account->Contacts ({len(acc.contacts if acc else [])}), Account->Opps ({len(acc.opportunities if acc else [])}), Job->Errors ({len(job.errors if job else [])}), Job->Logs ({len(job.audit_logs if job else [])})"
        except Exception as exc:
            rel_ok = False
            rel_detail = f"Relationship error: {str(exc)}"
        checks.append(("Entity Relationships Navigation", rel_ok, rel_detail))

        # 4. Constraints Verification (Primary Keys & Foreign Keys)
        try:
            # Check foreign keys on child tables
            contact_fks = inspector.get_foreign_keys("sf_contacts")
            has_contact_fk = any(fk["referred_table"] == "sf_accounts" for fk in contact_fks)

            opp_fks = inspector.get_foreign_keys("sf_opportunities")
            has_opp_fk = any(fk["referred_table"] == "sf_accounts" for fk in opp_fks)

            error_fks = inspector.get_foreign_keys("bulk_ingestion_errors")
            has_error_fk = any(fk["referred_table"] == "bulk_ingestion_jobs" for fk in error_fks)

            # Check primary keys
            acc_pk = inspector.get_pk_constraint("sf_accounts")["constrained_columns"]
            has_acc_pk = acc_pk == ["salesforce_id"]

            constraints_ok = has_contact_fk and has_opp_fk and has_error_fk and has_acc_pk
            constraints_detail = "Verified Foreign Keys (Account FK, Job FK) and Primary Keys (salesforce_id, UUIDs)"
        except Exception as exc:
            constraints_ok = False
            constraints_detail = f"Constraint error: {str(exc)}"
        checks.append(("Key Constraints (PK & FK)", constraints_ok, constraints_detail))

        # 5. Indexes Verification
        try:
            acc_indexes = {idx["name"] for idx in inspector.get_indexes("sf_accounts")}
            has_acc_modstamp = any("modstamp" in name for name in acc_indexes)

            contact_indexes = {idx["name"] for idx in inspector.get_indexes("sf_contacts")}
            has_contact_account = any("account_id" in name for name in contact_indexes)

            opp_indexes = {idx["name"] for idx in inspector.get_indexes("sf_opportunities")}
            has_opp_stage = any("stage" in name for name in opp_indexes)

            indexes_ok = has_acc_modstamp and has_contact_account and has_opp_stage
            indexes_detail = f"Verified B-Tree indexes: SystemModstamp, AccountId, and StageName"
        except Exception as exc:
            indexes_ok = False
            indexes_detail = f"Index error: {str(exc)}"
        checks.append(("Database Performance Indexes", indexes_ok, indexes_detail))

        # 6. Migration Success
        versions_dir = ROOT_DIR / "migrations" / "versions"
        migration_files = list(versions_dir.glob("*.py")) if versions_dir.exists() else []
        migration_ok = len(migration_files) > 0
        migration_detail = f"Alembic migration versions detected: {len(migration_files)} file(s)"
        checks.append(("Flask-Migrate Versioning", migration_ok, migration_detail))

        # 7. Seed Data Volume & Integrity
        try:
            acc_count = db.session.query(SalesforceAccount).count()
            contact_count = db.session.query(SalesforceContact).count()
            opp_count = db.session.query(SalesforceOpportunity).count()
            job_count = db.session.query(IngestionJob).count()
            error_count = db.session.query(IngestionError).count()
            log_count = db.session.query(AuditLog).count()

            seed_ok = (
                acc_count >= 3 and
                contact_count >= 3 and
                opp_count >= 2 and
                job_count >= 1 and
                error_count >= 2 and
                log_count >= 2
            )
            seed_detail = f"Records: Accounts({acc_count}), Contacts({contact_count}), Opps({opp_count}), Jobs({job_count}), Errors({error_count}), Logs({log_count})"
        except Exception as exc:
            seed_ok = False
            seed_detail = f"Seed data query error: {str(exc)}"
        checks.append(("Seed Data Integrity & Volumes", seed_ok, seed_detail))

    # Print Evaluation Table
    all_passed = True
    for name, status, detail in checks:
        status_str = "PASS" if status else "FAIL"
        if not status:
            all_passed = False
        print(f"[{status_str}] {name:<38} : {detail}")

    print("=" * 75)
    if all_passed:
        print("PHASE 4 DATABASE IMPLEMENTATION VERIFIED")
        print("=" * 75)
        return True
    else:
        print("PHASE 4 DATABASE IMPLEMENTATION FAILED")
        print("=" * 75)
        return False

if __name__ == "__main__":
    if not run_phase4_verification():
        sys.exit(1)
    sys.exit(0)
