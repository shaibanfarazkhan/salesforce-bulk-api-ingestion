# Salesforce Bulk API 2.0 Data Ingestion System

Enterprise-grade, asynchronous data pipeline platform designed to ingest high-volume Salesforce CRM records into PostgreSQL 17 using modern Clean Architecture principles, streaming CSV parsing, and idempotent database upserts.

---

## Technical Stack
- **Language:** Python 3.13
- **Framework:** Flask 3.x
- **Persistence:** SQLAlchemy 2.0 & PostgreSQL 17
- **Serialization & DTOs:** Marshmallow 3.x
- **Data Streaming:** Pandas 2.x
- **HTTP Client:** Requests with retry adapters
- **Containerization:** Docker & Docker Compose
- **Testing:** Pytest

---

## Clean Architecture Directory Layout

```
Salesforce Bulk API Data Ingestion System/
├── app/
│   ├── __init__.py                 # Application Factory (create_app)
│   ├── config.py                   # 12-Factor Configuration
│   ├── api/
│   │   └── routes.py               # Blueprint route declarations
│   ├── controllers/                # Thin HTTP controllers
│   │   ├── auth_controller.py
│   │   ├── bulk_controller.py
│   │   └── health_controller.py
│   ├── services/                   # Business use-case services
│   │   ├── auth_service.py
│   │   ├── bulk_service.py
│   │   ├── polling_service.py
│   │   ├── csv_stream_service.py
│   │   └── validation_service.py
│   ├── repositories/               # Persistence abstraction
│   │   └── bulk_repository.py
│   ├── database/                   # Persistence engine & ORM models
│   │   ├── database.py
│   │   └── models.py
│   ├── schemas/                    # Marshmallow DTO contracts
│   │   ├── auth_schema.py
│   │   └── bulk_schema.py
│   ├── middleware/                 # Interceptors & Logging
│   │   ├── error_handler.py
│   │   └── logging.py
│   ├── utils/                      # Constants, exceptions & helpers
│   │   ├── constants.py
│   │   ├── exceptions.py
│   │   └── helpers.py
│   └── integrations/               # External Salesforce client
│       └── salesforce_client.py
├── migrations/                     # Database migrations
├── tests/                          # Automated Pytest suite
│   ├── conftest.py
│   └── test_health.py
├── scripts/                        # Operational & Verification tooling
│   ├── verify_phase1_requirements.py
│   ├── verify_phase2_design.py
│   └── verify_phase3_implementation.py
├── run.py                          # WSGI runner
├── requirements.txt                # Production dependencies
├── Dockerfile                      # Multi-stage production Docker container
├── docker-compose.yml              # Local development stack
├── .env.example                    # Environment variable template
└── README.md
```

---

## Getting Started

### 1. Environment Configuration
```powershell
cp .env.example .env
```

### 2. Run Verification Script
```powershell
python scripts/verify_phase3_implementation.py
```

### 3. Run Automated Tests
```powershell
pytest -v
```

### 4. Start Local Development Server
```powershell
python run.py
```

The application will be available at `http://localhost:5000`.

---

## API Endpoints (Phase 3 Skeleton)
- `GET /health` - Health check probe
- `GET /metrics` - Operational telemetry
- `POST /login` - OAuth 2.0 login
- `POST /refresh` - OAuth token refresh
- `POST /bulk/jobs` - Create Bulk query job
- `GET /bulk/jobs` - List bulk query jobs
- `GET /bulk/jobs/<id>` - Inspect bulk job status
- `POST /ingest` - Trigger ingestion pipeline
- `GET /logs` - View system logs
