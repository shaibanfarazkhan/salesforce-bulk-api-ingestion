"""
Enterprise Database Seeder for Development & Staging Environments.
Populates normalized tables with realistic Salesforce CRM entities,
audit job ledgers, dead-letter quarantine records, and operational telemetry logs.
"""

import sys
import uuid
from decimal import Decimal
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

# Ensure root directory is on sys.path
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import create_app
from app.database.database import db
from app.database.models import (
    IngestionJob,
    IngestionError,
    SalesforceAccount,
    SalesforceContact,
    SalesforceOpportunity,
    AuditLog
)

def seed_database(drop_existing: bool = False) -> bool:
    """
    Populate database with enterprise test data.
    
    Args:
        drop_existing: If True, drop and recreate all tables before seeding.
    """
    app = create_app("development")
    
    with app.app_context():
        print("=" * 75)
        print("SALESFORCE BULK API 2.0 DATA INGESTION - SEED DATA POPULATION")
        print("=" * 75)
        
        if drop_existing:
            print("[*] Recreating database schema tables...")
            db.drop_all()
            db.create_all()
            print("  [SUCCESS] Schema recreated.")

        # Check if accounts already exist
        existing_count = db.session.query(SalesforceAccount).count()
        if existing_count > 0 and not drop_existing:
            print(f"[*] Found {existing_count} existing accounts. Skipping duplicate seed insertion.")
            return True

        now_utc = datetime.now(timezone.utc)
        
        try:
            # 1. Seed Accounts (18-character case-insensitive Salesforce IDs)
            print("[*] Seeding Salesforce Accounts...")
            acc1 = SalesforceAccount(
                salesforce_id="001300000001AAA001",
                name="Acme Corporation Global",
                account_number="ACC-98421",
                account_type="Customer - Direct",
                industry="Manufacturing & Cloud Services",
                annual_revenue=Decimal("15000000.00"),
                phone="+1 (555) 019-2834",
                website="https://www.acme-global.example.com",
                billing_city="San Francisco",
                billing_country="United States",
                sf_created_date=now_utc - timedelta(days=90),
                system_modstamp=now_utc - timedelta(minutes=15),
                is_deleted=False
            )
            acc2 = SalesforceAccount(
                salesforce_id="001300000001BBB002",
                name="Apex Global Logistics & Shipping",
                account_number="ACC-55192",
                account_type="Customer - Channel",
                industry="Transportation & Supply Chain",
                annual_revenue=Decimal("42500000.50"),
                phone="+1 (555) 088-7711",
                website="https://www.apexlogistics.example.com",
                billing_city="Chicago",
                billing_country="United States",
                sf_created_date=now_utc - timedelta(days=120),
                system_modstamp=now_utc - timedelta(hours=2),
                is_deleted=False
            )
            acc3 = SalesforceAccount(
                salesforce_id="001300000001CCC003",
                name="Nova Health Solutions Inc",
                account_number="ACC-12093",
                account_type="Prospect",
                industry="Healthcare & Life Sciences",
                annual_revenue=Decimal("8900000.00"),
                phone="+1 (555) 432-8899",
                website="https://www.novahealth.example.com",
                billing_city="Boston",
                billing_country="United States",
                sf_created_date=now_utc - timedelta(days=30),
                system_modstamp=now_utc - timedelta(hours=5),
                is_deleted=False
            )
            db.session.add_all([acc1, acc2, acc3])
            db.session.flush()
            print(f"  [SUCCESS] Inserted 3 Accounts.")

            # 2. Seed Contacts
            print("[*] Seeding Salesforce Contacts...")
            c1 = SalesforceContact(
                salesforce_id="003300000001AAA101",
                account_id=acc1.salesforce_id,
                first_name="Sarah",
                last_name="Connor",
                email="s.connor@acme-global.example.com",
                phone="+1 (555) 019-2835",
                title="VP of Global Procurement",
                department="Operations",
                lead_source="Enterprise Referral",
                sf_created_date=now_utc - timedelta(days=80),
                system_modstamp=now_utc - timedelta(minutes=10),
                is_deleted=False
            )
            c2 = SalesforceContact(
                salesforce_id="003300000001AAA102",
                account_id=acc1.salesforce_id,
                first_name="Marcus",
                last_name="Wright",
                email="m.wright@acme-global.example.com",
                phone="+1 (555) 019-2836",
                title="Lead Infrastructure Architect",
                department="Information Technology",
                lead_source="Dreamforce Conference",
                sf_created_date=now_utc - timedelta(days=75),
                system_modstamp=now_utc - timedelta(minutes=10),
                is_deleted=False
            )
            c3 = SalesforceContact(
                salesforce_id="003300000001BBB103",
                account_id=acc2.salesforce_id,
                first_name="Elena",
                last_name="Rostova",
                email="e.rostova@apexlogistics.example.com",
                phone="+1 (555) 088-7712",
                title="Chief Technology Officer",
                department="Executive Leadership",
                lead_source="Inbound Web Form",
                sf_created_date=now_utc - timedelta(days=110),
                system_modstamp=now_utc - timedelta(hours=1),
                is_deleted=False
            )
            db.session.add_all([c1, c2, c3])
            db.session.flush()
            print(f"  [SUCCESS] Inserted 3 Contacts.")

            # 3. Seed Opportunities
            print("[*] Seeding Salesforce Opportunities...")
            opp1 = SalesforceOpportunity(
                salesforce_id="006300000001AAA201",
                account_id=acc1.salesforce_id,
                name="Acme Q4 Enterprise Platform Renewal",
                stage_name="Closed Won",
                amount=Decimal("1250000.00"),
                probability=Decimal("100.00"),
                close_date=date.today(),
                opportunity_type="Existing Customer - Renewal",
                sf_created_date=now_utc - timedelta(days=60),
                system_modstamp=now_utc - timedelta(minutes=5),
                is_deleted=False
            )
            opp2 = SalesforceOpportunity(
                salesforce_id="006300000001BBB202",
                account_id=acc2.salesforce_id,
                name="Apex Global Warehouse IoT Integration",
                stage_name="Negotiation/Review",
                amount=Decimal("850000.00"),
                probability=Decimal("80.00"),
                close_date=date.today() + timedelta(days=30),
                opportunity_type="New Business",
                sf_created_date=now_utc - timedelta(days=45),
                system_modstamp=now_utc - timedelta(hours=1),
                is_deleted=False
            )
            db.session.add_all([opp1, opp2])
            db.session.flush()
            print(f"  [SUCCESS] Inserted 2 Opportunities.")

            # 4. Seed Bulk Ingestion Jobs & Telemetry
            print("[*] Seeding Bulk Ingestion Jobs & Audit Ledgers...")
            job_completed = IngestionJob(
                sf_job_id="750300000001AAA999",
                target_object="Account",
                operation="query",
                soql_query="SELECT Id, Name, AccountNumber, Type, Industry, AnnualRevenue, SystemModstamp FROM Account",
                status="COMPLETED",
                records_processed=10000,
                records_ingested=9998,
                records_failed=2,
                execution_time_ms=14320,
                started_at=now_utc - timedelta(minutes=20),
                completed_at=now_utc - timedelta(minutes=15)
            )
            db.session.add(job_completed)
            db.session.flush()

            # 5. Seed Dead-Letter Quarantine Errors
            err1 = IngestionError(
                job_id=job_completed.id,
                sf_record_id="001300000009ERR001",
                target_object="Account",
                raw_payload={"Id": "001300000009ERR001", "Name": "", "AnnualRevenue": "NOT_A_NUMBER"},
                error_code="TYPE_COERCION_FAILURE",
                error_message="Invalid numeric syntax for column AnnualRevenue: 'NOT_A_NUMBER'"
            )
            err2 = IngestionError(
                job_id=job_completed.id,
                sf_record_id="001300000009ERR002",
                target_object="Account",
                raw_payload={"Id": "001300000009ERR002", "SystemModstamp": "invalid-timestamp"},
                error_code="SCHEMA_CONSTRAINT_VIOLATION",
                error_message="Column 'name' is non-nullable but was missing from source row"
            )
            db.session.add_all([err1, err2])

            # 6. Seed Operational Audit Logs
            log1 = AuditLog(
                job_id=job_completed.id,
                action="JOB_SUBMITTED",
                entity_type="Account",
                record_count=0,
                message="Bulk API 2.0 query job successfully submitted to Salesforce",
                payload={"sf_job_id": job_completed.sf_job_id, "operation": "query"}
            )
            log2 = AuditLog(
                job_id=job_completed.id,
                action="STREAM_PROCESSED",
                entity_type="Account",
                record_count=10000,
                message="CSV stream completed: 9,998 rows upserted, 2 rows quarantined to dead-letter",
                payload={"latency_ms": 14320, "quarantined_count": 2}
            )
            db.session.add_all([log1, log2])

            # Commit all seed data in single transaction
            db.session.commit()
            print("  [SUCCESS] Inserted IngestionJob, IngestionErrors, and AuditLogs.")
            print("=" * 75)
            print("DATABASE SEEDING COMPLETED SUCCESSFULLY")
            print("=" * 75)
            return True

        except Exception as exc:
            db.session.rollback()
            print(f"[FAIL] Error seeding database: {str(exc)}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    drop_flag = "--drop" in sys.argv
    if not seed_database(drop_existing=drop_flag):
        sys.exit(1)
    sys.exit(0)
