"""
Database Persistence, Engine Configuration & Migration Lifecycle.
Configures SQLAlchemy 2.0 with enterprise naming conventions, connection pooling,
scoped sessions, and Flask-Migrate integration.
"""

from typing import Dict, Any
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import text, MetaData
from sqlalchemy.orm import DeclarativeBase

# Enterprise naming convention for deterministic Alembic constraint migrations
naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

metadata = MetaData(naming_convention=naming_convention)

class Base(DeclarativeBase):
    """Declarative base class for all SQLAlchemy domain models with naming convention."""
    metadata = metadata

# Singleton SQLAlchemy and Flask-Migrate instances
db = SQLAlchemy(model_class=Base)
migrate = Migrate()

def init_db(app: Flask) -> SQLAlchemy:
    """
    Initialize SQLAlchemy and Flask-Migrate extensions with Flask application context.
    
    Args:
        app: Configured Flask application instance.
    
    Returns:
        db: Initialized SQLAlchemy singleton.
    """
    db.init_app(app)
    migrate.init_app(app, db, directory="migrations")
    app.logger.info("SQLAlchemy and Flask-Migrate extensions successfully initialized.")
    return db

def check_db_health() -> Dict[str, Any]:
    """
    Execute a low-overhead liveness probe (SELECT 1) against the database engine.
    
    Returns:
        dict: Health check outcome with status and response latency.
    """
    try:
        with db.engine.connect() as connection:
            result = connection.execute(text("SELECT 1")).scalar()
            if result == 1:
                return {"status": "UP", "dialect": db.engine.dialect.name}
            return {"status": "DOWN", "error": "Query did not return expected value"}
    except Exception as exc:
        return {"status": "DOWN", "error": str(exc)}
