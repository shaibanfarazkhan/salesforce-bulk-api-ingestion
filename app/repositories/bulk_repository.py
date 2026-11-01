"""
Enterprise Repository Layer for Bulk Ingestion & High-Throughput Upserts.
Implements idempotent PostgreSQL 17 ON CONFLICT DO UPDATE upserts with
deterministic primary key sorting to eliminate deadlocks, and supports
SQLite in-memory test dialect fallback.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import uuid
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.database.database import db
from app.database.models import (
    IngestionJob,
    IngestionError,
    SalesforceAccount,
    SalesforceContact,
    SalesforceOpportunity,
    AuditLog
)
from app.utils.exceptions import RepositoryException

logger = logging.getLogger(__name__)


class BaseRepository(ABC):
    """Abstract Base Repository defining the standard contract for data access operations."""

    @abstractmethod
    def create_job(self, job: IngestionJob) -> IngestionJob:
        pass

    @abstractmethod
    def update_job(self, job: IngestionJob) -> IngestionJob:
        pass

    @abstractmethod
    def get_by_id(self, entity_id: Any) -> Optional[IngestionJob]:
        pass

    @abstractmethod
    def list_jobs(self, limit: int = 50, offset: int = 0) -> List[IngestionJob]:
        pass


class BulkRepository(BaseRepository):
    """
    Data Access Repository managing ingestion audit ledgers, dead-letter rows,
    and high-throughput batched upserts into CRM tables.
    """

    def __init__(self, session: Optional[Session] = None) -> None:
        self._session = session

    @property
    def session(self) -> Session:
        return self._session or db.session

    # ==========================================================================
    # IngestionJob Lifecycle Audit Methods
    # ==========================================================================
    def create_job(self, job: IngestionJob) -> IngestionJob:
        """Persist a new Bulk Ingestion Job audit record."""
        try:
            self.session.add(job)
            self.session.commit()
            logger.info(f"Created IngestionJob audit record: id={job.id}, sf_job_id={job.sf_job_id}")
            return job
        except Exception as exc:
            self.session.rollback()
            logger.error(f"Failed to create IngestionJob: {str(exc)}")
            raise RepositoryException(f"Failed to create IngestionJob: {str(exc)}") from exc

    def update_job(self, job: IngestionJob) -> IngestionJob:
        """Update an existing IngestionJob record."""
        try:
            job.updated_at = datetime.now(timezone.utc)
            self.session.merge(job)
            self.session.commit()
            return job
        except Exception as exc:
            self.session.rollback()
            logger.error(f"Failed to update IngestionJob {job.id}: {str(exc)}")
            raise RepositoryException(f"Failed to update IngestionJob: {str(exc)}") from exc

    def get_by_id(self, entity_id: Any) -> Optional[IngestionJob]:
        """Retrieve an IngestionJob by internal UUID primary key."""
        try:
            target_id = entity_id
            if isinstance(entity_id, str):
                try:
                    target_id = uuid.UUID(entity_id)
                except (ValueError, TypeError):
                    return None
            return self.session.query(IngestionJob).filter_by(id=target_id).first()
        except Exception as exc:
            logger.error(f"Failed to query IngestionJob by id {entity_id}: {str(exc)}")
            raise RepositoryException(f"Error querying IngestionJob by id: {str(exc)}") from exc

    def get_by_sf_job_id(self, sf_job_id: str) -> Optional[IngestionJob]:
        """Retrieve an IngestionJob by Salesforce 750... job identifier."""
        try:
            return self.session.query(IngestionJob).filter_by(sf_job_id=sf_job_id).first()
        except Exception as exc:
            logger.error(f"Failed to query IngestionJob by sf_job_id {sf_job_id}: {str(exc)}")
            raise RepositoryException(f"Error querying IngestionJob by sf_job_id: {str(exc)}") from exc

    def list_jobs(self, limit: int = 50, offset: int = 0) -> List[IngestionJob]:
        """Retrieve paginated list of historical ingestion jobs ordered descending by creation date."""
        try:
            return (
                self.session.query(IngestionJob)
                .order_by(IngestionJob.created_at.desc())
                .limit(limit)
                .offset(offset)
                .all()
            )
        except Exception as exc:
            logger.error(f"Failed to list IngestionJobs: {str(exc)}")
            raise RepositoryException(f"Error listing IngestionJobs: {str(exc)}") from exc

    # ==========================================================================
    # High-Throughput Idempotent Bulk Upsert Operations
    # ==========================================================================
    def bulk_upsert_accounts(self, records: List[Dict[str, Any]]) -> int:
        """
        Idempotent bulk upsert into sf_accounts using ON CONFLICT DO UPDATE.
        Records are deterministically sorted by salesforce_id to prevent deadlocks.
        """
        if not records:
            return 0
        return self._execute_upsert(
            model=SalesforceAccount,
            pk_col="salesforce_id",
            records=records,
            excluded_update_cols={"salesforce_id", "sf_created_date"}
        )

    def bulk_upsert_contacts(self, records: List[Dict[str, Any]]) -> int:
        """
        Idempotent bulk upsert into sf_contacts using ON CONFLICT DO UPDATE.
        """
        if not records:
            return 0
        return self._execute_upsert(
            model=SalesforceContact,
            pk_col="salesforce_id",
            records=records,
            excluded_update_cols={"salesforce_id", "sf_created_date"}
        )

    def bulk_upsert_opportunities(self, records: List[Dict[str, Any]]) -> int:
        """
        Idempotent bulk upsert into sf_opportunities using ON CONFLICT DO UPDATE.
        """
        if not records:
            return 0
        return self._execute_upsert(
            model=SalesforceOpportunity,
            pk_col="salesforce_id",
            records=records,
            excluded_update_cols={"salesforce_id", "sf_created_date"}
        )

    def _execute_upsert(
        self,
        model: Any,
        pk_col: str,
        records: List[Dict[str, Any]],
        excluded_update_cols: set
    ) -> int:
        """
        Generic dialect-aware ON CONFLICT DO UPDATE execution engine.
        Supports both PostgreSQL 17 and SQLite with dead-lock prevention sorting.
        """
        # 1. Deterministic Primary Key Sorting to prevent deadlocks
        sorted_records = sorted(records, key=lambda r: str(r.get(pk_col, "")))
        dialect_name = self.session.bind.dialect.name if self.session.bind else "postgresql"

        try:
            if dialect_name == "postgresql":
                insert_fn = pg_insert
            else:
                insert_fn = sqlite_insert

            stmt = insert_fn(model).values(sorted_records)
            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in model.__table__.columns
                if c.name not in excluded_update_cols
            }

            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=[getattr(model, pk_col)],
                set_=update_cols
            )

            result = self.session.execute(upsert_stmt)
            self.session.commit()
            affected = len(sorted_records)
            logger.info(f"Upserted {affected} rows into {model.__tablename__}")
            return affected

        except Exception as exc:
            self.session.rollback()
            logger.error(f"Bulk upsert into {model.__tablename__} failed: {str(exc)}")
            raise RepositoryException(f"Bulk upsert failed on {model.__tablename__}: {str(exc)}") from exc

    # ==========================================================================
    # Dead-Letter Quarantine & Audit Logging Persistence
    # ==========================================================================
    def insert_errors(self, errors: List[Dict[str, Any]]) -> int:
        """Bulk insert quarantined rows into bulk_ingestion_errors."""
        if not errors:
            return 0
        try:
            err_objects = [IngestionError(**err) for err in errors]
            self.session.bulk_save_objects(err_objects)
            self.session.commit()
            logger.warning(f"Quarantined {len(errors)} records into bulk_ingestion_errors.")
            return len(errors)
        except Exception as exc:
            self.session.rollback()
            logger.error(f"Failed to persist dead-letter errors: {str(exc)}")
            raise RepositoryException(f"Failed to insert errors: {str(exc)}") from exc

    def insert_logs(self, logs: List[Dict[str, Any]]) -> int:
        """Bulk insert audit events into audit_logs."""
        if not logs:
            return 0
        try:
            log_objects = [AuditLog(**entry) for entry in logs]
            self.session.bulk_save_objects(log_objects)
            self.session.commit()
            return len(logs)
        except Exception as exc:
            self.session.rollback()
            logger.error(f"Failed to persist audit logs: {str(exc)}")
            raise RepositoryException(f"Failed to insert audit logs: {str(exc)}") from exc
