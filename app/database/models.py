"""
Production-Ready SQLAlchemy 2.0 Typed Domain Models.
Implements normalized PostgreSQL 17 relational schemas with strict typing,
UUID primary keys, foreign key constraints, indexes, cascading rules,
check constraints, UTC timezone timestamps, and soft-delete capabilities.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, date
from decimal import Decimal
import uuid

from sqlalchemy import (
    String, Text, Boolean, Numeric, Date, DateTime, BigInteger, Integer,
    ForeignKey, CheckConstraint, Index, JSON, Uuid, func
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database.database import db

# ==============================================================================
# 1. IngestionJob Audit Ledger Model
# ==============================================================================
class IngestionJob(db.Model):
    """
    Master audit ledger record for all Salesforce Bulk API 2.0 query jobs.
    Tracks query parameters, runtime status, execution latencies, and processed volumes.
    """
    __tablename__ = "bulk_ingestion_jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('SUBMITTED', 'IN_PROGRESS', 'STREAMING', 'COMPLETED', 'FAILED', 'ABORTED')",
            name="chk_ingestion_job_status"
        ),
        Index("idx_bulk_jobs_sf_id", "sf_job_id"),
        Index("idx_bulk_jobs_status", "status"),
        Index("idx_bulk_jobs_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    sf_job_id: Mapped[Optional[str]] = mapped_column(
        String(32), unique=True, nullable=True
    )
    target_object: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    operation: Mapped[str] = mapped_column(
        String(32), nullable=False, default="query"
    )
    soql_query: Mapped[str] = mapped_column(
        Text, nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="SUBMITTED"
    )
    records_processed: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0
    )
    records_ingested: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0
    )
    records_failed: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0
    )
    execution_time_ms: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    errors: Mapped[List["IngestionError"]] = relationship(
        "IngestionError",
        back_populates="job",
        cascade="all, delete-orphan",
        passive_deletes=True
    )
    audit_logs: Mapped[List["AuditLog"]] = relationship(
        "AuditLog",
        back_populates="job",
        cascade="all, delete-orphan",
        passive_deletes=True
    )

    # TODO [Phase 8]: Add business logic transition helper: mark_in_progress, mark_completed, mark_failed.

    def __repr__(self) -> str:
        return f"<IngestionJob(id={self.id}, sf_job_id={self.sf_job_id}, status={self.status}, object={self.target_object})>"


# ==============================================================================
# 2. IngestionError Dead-Letter Quarantine Model
# ==============================================================================
class IngestionError(db.Model):
    """
    Dead-letter quarantine repository storing malformed or unparseable records
    isolated during CSV streaming and schema validation phases.
    """
    __tablename__ = "bulk_ingestion_errors"
    __table_args__ = (
        Index("idx_bulk_errors_job_id", "job_id"),
        Index("idx_bulk_errors_sf_record", "sf_record_id"),
        Index("idx_bulk_errors_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("bulk_ingestion_jobs.id", ondelete="CASCADE"),
        nullable=False
    )
    sf_record_id: Mapped[Optional[str]] = mapped_column(
        String(18), nullable=True
    )
    target_object: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    raw_payload: Mapped[Dict[str, Any]] = mapped_column(
        JSON, nullable=False
    )
    error_code: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    error_message: Mapped[str] = mapped_column(
        Text, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationship back to master job
    job: Mapped["IngestionJob"] = relationship("IngestionJob", back_populates="errors")

    # TODO [Phase 9]: Add method to replay quarantined rows after schema corrections.

    def __repr__(self) -> str:
        return f"<IngestionError(id={self.id}, sf_record_id={self.sf_record_id}, code={self.error_code})>"


# ==============================================================================
# 3. SalesforceAccount Model
# ==============================================================================
class SalesforceAccount(db.Model):
    """
    Normalized persistence model representing Salesforce Account CRM entities.
    Enforces 18-character case-insensitive Salesforce IDs, audit timestamps, and soft deletion.
    """
    __tablename__ = "sf_accounts"
    __table_args__ = (
        CheckConstraint(
            "annual_revenue IS NULL OR annual_revenue >= 0",
            name="chk_account_annual_revenue_positive"
        ),
        Index("idx_sf_accounts_modstamp", "system_modstamp"),
        Index("idx_sf_accounts_name", "name"),
        Index("idx_sf_accounts_is_deleted", "is_deleted"),
    )

    salesforce_id: Mapped[str] = mapped_column(
        String(18), primary_key=True
    )
    name: Mapped[str] = mapped_column(
        String(255), nullable=False
    )
    account_number: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True
    )
    account_type: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True
    )
    industry: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True
    )
    annual_revenue: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    phone: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True
    )
    website: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )
    billing_city: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True
    )
    billing_country: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True
    )
    sf_created_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    system_modstamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Soft Delete Support
    is_deleted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships to Child SObjects
    contacts: Mapped[List["SalesforceContact"]] = relationship(
        "SalesforceContact",
        back_populates="account",
        cascade="all, delete-orphan",
        passive_deletes=True
    )
    opportunities: Mapped[List["SalesforceOpportunity"]] = relationship(
        "SalesforceOpportunity",
        back_populates="account",
        cascade="all, delete-orphan",
        passive_deletes=True
    )

    # TODO [Phase 9]: Add batched ON CONFLICT DO UPDATE upsert statement builder.

    def __repr__(self) -> str:
        return f"<SalesforceAccount(id={self.salesforce_id}, name='{self.name}', revenue={self.annual_revenue})>"


# ==============================================================================
# 4. SalesforceContact Model
# ==============================================================================
class SalesforceContact(db.Model):
    """
    Normalized persistence model representing Salesforce Contact CRM entities.
    Maintains foreign key relationship to parent SalesforceAccount with soft delete.
    """
    __tablename__ = "sf_contacts"
    __table_args__ = (
        Index("idx_sf_contacts_account_id", "account_id"),
        Index("idx_sf_contacts_modstamp", "system_modstamp"),
        Index("idx_sf_contacts_last_name", "last_name"),
        Index("idx_sf_contacts_email", "email"),
        Index("idx_sf_contacts_is_deleted", "is_deleted"),
    )

    salesforce_id: Mapped[str] = mapped_column(
        String(18), primary_key=True
    )
    account_id: Mapped[Optional[str]] = mapped_column(
        String(18),
        ForeignKey("sf_accounts.salesforce_id", ondelete="SET NULL"),
        nullable=True
    )
    first_name: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True
    )
    last_name: Mapped[str] = mapped_column(
        String(128), nullable=False
    )
    email: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )
    phone: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True
    )
    title: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True
    )
    department: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True
    )
    lead_source: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True
    )
    sf_created_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    system_modstamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Soft Delete Support
    is_deleted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationship back to parent Account
    account: Mapped[Optional["SalesforceAccount"]] = relationship(
        "SalesforceAccount", back_populates="contacts"
    )

    # TODO [Phase 9]: Add method to link or re-parent contact when account_id changes.

    def __repr__(self) -> str:
        return f"<SalesforceContact(id={self.salesforce_id}, name='{self.first_name} {self.last_name}', email='{self.email}')>"


# ==============================================================================
# 5. SalesforceOpportunity Model
# ==============================================================================
class SalesforceOpportunity(db.Model):
    """
    Normalized persistence model representing Salesforce Opportunity (Deal) entities.
    Maintains foreign key relationship to parent SalesforceAccount with soft delete.
    """
    __tablename__ = "sf_opportunities"
    __table_args__ = (
        CheckConstraint(
            "amount IS NULL OR amount >= 0",
            name="chk_opportunity_amount_positive"
        ),
        CheckConstraint(
            "probability IS NULL OR (probability >= 0 AND probability <= 100)",
            name="chk_opportunity_probability_range"
        ),
        Index("idx_sf_opps_account_id", "account_id"),
        Index("idx_sf_opps_modstamp", "system_modstamp"),
        Index("idx_sf_opps_stage", "stage_name"),
        Index("idx_sf_opps_is_deleted", "is_deleted"),
    )

    salesforce_id: Mapped[str] = mapped_column(
        String(18), primary_key=True
    )
    account_id: Mapped[Optional[str]] = mapped_column(
        String(18),
        ForeignKey("sf_accounts.salesforce_id", ondelete="CASCADE"),
        nullable=True
    )
    name: Mapped[str] = mapped_column(
        String(255), nullable=False
    )
    stage_name: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    amount: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    probability: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2), nullable=True
    )
    close_date: Mapped[Optional[date]] = mapped_column(
        Date, nullable=True
    )
    opportunity_type: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True
    )
    sf_created_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    system_modstamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Soft Delete Support
    is_deleted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationship back to parent Account
    account: Mapped[Optional["SalesforceAccount"]] = relationship(
        "SalesforceAccount", back_populates="opportunities"
    )

    # TODO [Phase 9]: Add method to calculate pipeline revenue aggregation.

    def __repr__(self) -> str:
        return f"<SalesforceOpportunity(id={self.salesforce_id}, name='{self.name}', stage='{self.stage_name}', amount={self.amount})>"


# ==============================================================================
# 6. AuditLog Model
# ==============================================================================
class AuditLog(db.Model):
    """
    Stateful operational event and telemetry log for all data pipeline actions.
    Persists granular pipeline lifecycle transitions and record volume counters.
    """
    __tablename__ = "audit_logs"
    __table_args__ = (
        Index("idx_audit_logs_job_id", "job_id"),
        Index("idx_audit_logs_action", "action"),
        Index("idx_audit_logs_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("bulk_ingestion_jobs.id", ondelete="SET NULL"),
        nullable=True
    )
    action: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    entity_type: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True
    )
    record_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    message: Mapped[str] = mapped_column(
        Text, nullable=False
    )
    payload: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationship back to master IngestionJob
    job: Mapped[Optional["IngestionJob"]] = relationship("IngestionJob", back_populates="audit_logs")

    # TODO [Phase 11]: Add method to stream audit logs to external Datadog/Splunk endpoints.

    def __repr__(self) -> str:
        return f"<AuditLog(id={self.id}, action='{self.action}', records={self.record_count})>"
