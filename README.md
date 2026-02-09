# Tippers Services - FastAPI + Dagster Integration

A production-ready FastAPI + Dagster integration for orchestrating data workflows, featuring SQL query materialization, Snorkel labeling, and ML training pipelines.

## Project Structure

```
tippers-services/
├── backend/
│   ├── main.py                 # FastAPI application entry point
│   ├── routers/                # API endpoint routers
│   │   ├── catalog.py          # Asset catalog endpoints
│   │   ├── indexes.py          # Index CRUD and materialization
│   │   ├── rules.py            # Rule CRUD and materialization
│   │   ├── labeling_functions.py  # LF creation endpoints
│   │   ├── snorkel.py          # Snorkel training endpoints
│   │   └── dagster.py          # Dagster status endpoints
│   ├── utils/
│   │   ├── dagster_client.py   # GraphQL client for Dagster
│   │   └── validators.py       # SQL and Python validation
│   ├── db/
│   │   ├── session.py          # Database session management
│   │   └── models.py           # SQLAlchemy models
│   └── dagster_app/            # CRITICAL: Named 'dagster_app' to avoid module shadowing
│       ├── definitions.py      # Dagster definitions
│       ├── workspace.yaml      # Dagster workspace config
│       ├── resources.py        # Dagster resources (DB, S3, MLflow)
│       ├── jobs.py             # Dagster job definitions
│       └── assets/
│           ├── indexes.py      # Index materialization assets
│           ├── rules.py        # Rule materialization assets
│           └── snorkel.py      # Snorkel training assets
├── docker-compose.yml          # Multi-service Docker setup
├── Dockerfile                  # Backend service Docker image
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
└── README.md                   # This file
```

## Quick Start

### 1. Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- AWS account with S3 access (or LocalStack for testing)

### 2. Environment Setup

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` with your configuration:

```env
# AWS credentials for S3 storage
S3_BUCKET_NAME=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1
```

### 3. Start Services

Start all services using Docker Compose:

```bash
docker-compose up -d
```

This will start:
- **PostgreSQL** (port 5432) - Database
- **Redis** (port 6379) - Caching/state
- **FastAPI** (port 8000) - REST API
- **Dagster Daemon** - Background job processor
- **Dagster Webserver** (port 3000) - Dagster UI

### 4. Verify Services

Check that all services are running:

```bash
docker-compose ps
```

Test the FastAPI endpoint:

```bash
curl http://localhost:8000/health
```

Access the Dagster UI:

```
http://localhost:3000
```

## API Endpoints

### Asset Catalog

- `GET /concepts/{c_id}/catalog` - Get all indexes and rules for a concept

### Indexes

- `POST /concepts/{c_id}/indexes` - Create a new index definition
- `POST /concepts/{c_id}/indexes/{index_id}/materialize` - Trigger index materialization

### Rules

- `POST /concepts/{c_id}/rules` - Create a new rule definition
- `POST /concepts/{c_id}/rules/{r_id}/materialize` - Trigger rule materialization

### Labeling Functions

- `POST /concepts/{c_id}/labeling-functions/template` - Create template-based LF
- `POST /concepts/{c_id}/labeling-functions/regex` - Create regex-based LF
- `POST /concepts/{c_id}/labeling-functions/custom` - Create custom Python LF

### Snorkel Training

- `POST /concepts/{c_id}/snorkel/run` - Trigger Snorkel training pipeline
- `GET /concepts/{c_id}/snorkel/jobs/{job_id}` - Get training results

### Dagster

- `GET /dagster/runs/{run_id}` - Check Dagster job status

## Development

### Local Development Setup

1. Create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run FastAPI locally:

```bash
cd backend
uvicorn main:app --reload
```

4. Run Dagster locally:

```bash
dagster dev -w backend/dagster_app/workspace.yaml
```

### Database Migrations

To create database tables:

```python
from backend.db.session import init_db
init_db()
```

For production, use Alembic for migrations:

```bash
alembic init alembic
alembic revision --autogenerate -m "Initial migration"
alembic upgrade head
```

## Architecture

### Flow Overview

1. **User creates index/rule definitions** via FastAPI endpoints
2. **FastAPI validates and stores** definitions in PostgreSQL
3. **User triggers materialization** via API
4. **FastAPI sends job to Dagster** via GraphQL
5. **Dagster executes SQL queries** and uploads results to S3
6. **User builds pipeline** in React Flow UI
7. **User triggers Snorkel training** with selected assets
8. **Dagster trains model** and returns results (softmax or hard labels)

### Key Design Decisions

- **dagster_app directory name**: Prevents module shadowing issues with the `dagster` package
- **PYTHONPATH=/app**: Ensures imports work correctly in Docker
- **GraphQL client**: FastAPI communicates with Dagster via GraphQL API
- **Asset-based design**: Indexes and Rules are materialized, reusable Dagster assets
- **Softmax output**: Optional probabilistic predictions from Snorkel

## Docker Services

### FastAPI Service

- Serves REST API
- Validates SQL and Python code
- Triggers Dagster jobs
- Stores definitions in PostgreSQL

### Dagster Daemon

- Processes job queue
- Executes materialization jobs
- Runs Snorkel training pipelines

### Dagster Webserver

- Provides Dagster UI
- Exposes GraphQL API
- Shows job status and logs

## Troubleshooting

### Service won't start

Check logs:

```bash
docker-compose logs -f [service-name]
```

### Database connection issues

Ensure PostgreSQL is healthy:

```bash
docker-compose exec postgres pg_isready -U postgres
```

### Dagster job failures

1. Check Dagster UI: http://localhost:3000
2. View run logs in the UI
3. Check daemon logs: `docker-compose logs -f dagster-daemon`

### Import errors in Dagster

Verify `PYTHONPATH=/app` is set in docker-compose.yml

## Next Steps

1. Review the IMPLEMENTATION_GUIDE.md for detailed implementation
2. Set up your database schema (see guide)
3. Implement asset materialization logic
4. Build React Flow UI for visual pipeline building
5. Test with sample data

## Contributing

This is a template structure. To contribute:

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests
5. Submit a pull request

## License

[Your License Here]
