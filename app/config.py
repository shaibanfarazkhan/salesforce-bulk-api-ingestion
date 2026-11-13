"""
Enterprise Configuration Management.
Loads environment variables via python-dotenv following 12-factor principles.
Supports distinct Development, Testing, and Production environments.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Base directory of the repository
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file from root
load_dotenv(BASE_DIR / ".env")

class BaseConfig:
    """Base Configuration with default values and environment loading."""
    APP_NAME: str = "Salesforce Bulk API Ingestion System"
    VERSION: str = "1.0.0"
    ENVIRONMENT: str = os.getenv("FLASK_ENV", "development")
    DEBUG: bool = False
    TESTING: bool = False
    SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-insecure-enterprise-secret-key-change-in-prod")

    # Server Configuration
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "5000"))

    # Database Configuration (PostgreSQL 17)
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "sf_ingest_db")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres")

    # SQLAlchemy Configuration
    SQLALCHEMY_DATABASE_URI: str = os.getenv(
        "DATABASE_URL",
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS: bool = False
    SQLALCHEMY_ENGINE_OPTIONS: dict = (
        {} if "sqlite" in SQLALCHEMY_DATABASE_URI else {
            "pool_size": int(os.getenv("DB_POOL_SIZE", "10")),
            "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "20")),
            "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", "1800")),
            "pool_pre_ping": True,
        }
    )

    # Salesforce OAuth 2.0 Credentials
    SALESFORCE_CLIENT_ID: str = os.getenv("CLIENT_ID", "dummy_salesforce_client_id")
    SALESFORCE_CLIENT_SECRET: str = os.getenv("CLIENT_SECRET", "dummy_salesforce_client_secret")
    SALESFORCE_LOGIN_URL: str = os.getenv("LOGIN_URL", "https://login.salesforce.com")
    SALESFORCE_USERNAME: str = os.getenv("SF_USERNAME", "admin@enterprise.example.com")
    SALESFORCE_PASSWORD: str = os.getenv("SF_PASSWORD", "dummy_password")
    SALESFORCE_SECURITY_TOKEN: str = os.getenv("SF_SECURITY_TOKEN", "dummy_token")
    SALESFORCE_API_VERSION: str = os.getenv("SF_API_VERSION", "v62.0")

    # Ingestion & Polling Defaults
    CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", "10000"))
    POLL_MAX_ATTEMPTS: int = int(os.getenv("POLL_MAX_ATTEMPTS", "120"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR: str = os.getenv("LOG_DIR", str(BASE_DIR / "logs"))

    # Phase 7: Enterprise Processing, Resilience & Concurrency Configuration
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))
    QUEUE_MAX_SIZE: int = int(os.getenv("QUEUE_MAX_SIZE", "1000"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_BASE_DELAY: float = float(os.getenv("RETRY_BASE_DELAY", "2.0"))
    RETRY_MAX_DELAY: float = float(os.getenv("RETRY_MAX_DELAY", "60.0"))
    DLQ_MAX_SIZE: int = int(os.getenv("DLQ_MAX_SIZE", "5000"))
    DLQ_RETENTION_DAYS: int = int(os.getenv("DLQ_RETENTION_DAYS", "30"))
    SCHEDULER_ENABLED: bool = os.getenv("SCHEDULER_ENABLED", "True").lower() in ("true", "1", "yes")
    SCHEDULER_INTERVAL_SECONDS: int = int(os.getenv("SCHEDULER_INTERVAL_SECONDS", "300"))
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
    CIRCUIT_BREAKER_RESET_TIMEOUT: int = int(os.getenv("CIRCUIT_BREAKER_RESET_TIMEOUT", "60"))
    STRUCTURED_LOGGING: bool = os.getenv("STRUCTURED_LOGGING", "False").lower() in ("true", "1", "yes")

class DevelopmentConfig(BaseConfig):
    """Development Environment Configuration."""
    DEBUG: bool = True
    LOG_LEVEL: str = "DEBUG"

class TestingConfig(BaseConfig):
    """Testing Environment Configuration."""
    TESTING: bool = True
    DEBUG: bool = True
    SQLALCHEMY_DATABASE_URI: str = "sqlite:///:memory:"
    SQLALCHEMY_ENGINE_OPTIONS: dict = {}
    LOG_LEVEL: str = "WARNING"

class ProductionConfig(BaseConfig):
    """Production Environment Configuration."""
    DEBUG: bool = False
    TESTING: bool = False
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# Configuration Mapping Dictionary
CONFIG_MAP = {
    "development": DevelopmentConfig,
    "testing": TestingConfig,
    "production": ProductionConfig
}

def get_config(env_name: str = None) -> BaseConfig:
    """Retrieve appropriate configuration instance based on environment name."""
    env = env_name or os.getenv("FLASK_ENV", "development").lower()
    config_cls = CONFIG_MAP.get(env, DevelopmentConfig)
    return config_cls()
