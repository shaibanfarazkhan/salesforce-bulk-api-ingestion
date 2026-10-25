"""
Database Package Initialization.
Exports database instance, initialization hooks, and ORM models.
"""

from app.database.database import db, migrate, init_db, check_db_health
from app.database.models import (
    IngestionJob,
    IngestionError,
    SalesforceAccount,
    SalesforceContact,
    SalesforceOpportunity,
    AuditLog
)

__all__ = [
    "db",
    "migrate",
    "init_db",
    "check_db_health",
    "IngestionJob",
    "IngestionError",
    "SalesforceAccount",
    "SalesforceContact",
    "SalesforceOpportunity",
    "AuditLog"
]
