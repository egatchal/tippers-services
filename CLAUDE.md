# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tippers Services is a FastAPI + Dagster integration for orchestrating data workflows. It features SQL query materialization, Snorkel labeling, ML training pipelines, and WiFi-based occupancy analytics. The system uses a React Flow visual pipeline builder for the frontend.

## Development Commands

### Docker (Primary Development)

```bash
# Start all services
docker-compose up -d

# Restart services after code changes (no rebuild needed for Python file edits)
docker-compose restart fastapi dagster-webserver dagster-daemon

# Rebuild after adding new files or changing requirements.txt
docker-compose up -d --build fastapi dagster-webserver dagster-daemon

# Full restart (needed after DB schema changes)
docker-compose down && docker-compose up -d

# View logs
docker-compose logs -f [service-name]
```

### Local Development (Without Docker)

```bash
# Backend - Terminal 1
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000

# Dagster - Terminal 2
dagster dev -w backend/dagster_app/workspace.yaml

# Frontend - Terminal 3
cd frontend
npm install
npm run dev
```

### Frontend Commands

```bash
cd frontend
npm run dev      # Development server
npm run build    # Production build (tsc -b && vite build)
npm run lint     # ESLint
npm run preview  # Preview production build
```

## Architecture

### Service Ports
- **FastAPI**: 8000 (REST API, interactive docs at /docs)
- **Dagster Webserver**: 3000 (Dagster UI, GraphQL API)
- **PostgreSQL**: 5432
- **Redis**: 6379
- **MLflow**: 5001 (tracking server)

### Backend Structure (`backend/`)
- `main.py` - FastAPI application entry point, router registration
- `routers/` - API endpoint routers (catalog, indexes, rules, snorkel, occupancy, etc.)
- `db/` - SQLAlchemy models (`models.py`) and session management (`session.py`)
- `dagster_app/` - Dagster definitions, assets, jobs, resources, sensors
- `utils/` - Dagster GraphQL client, validators, timeout calculator
- `schemas.py` - Pydantic request/response schemas

### Frontend Structure (`frontend/src/`)
- `features/pipeline/` - Main pipeline builder (React Flow canvas, nodes, panels, stores)
- `features/connections/` - Database connection management
- `shared/` - Reusable components, hooks, API client

### Key Design Decisions
- **`dagster_app` directory name**: Named this way to avoid module shadowing with the `dagster` package
- **PYTHONPATH=/app**: Required in Docker for correct imports
- **GraphQL client**: FastAPI communicates with Dagster via GraphQL (`utils/dagster_client.py`)
- **Asset-based design**: Indexes, Rules, Features are materialized Dagster assets stored in S3

## Data Flow

1. **User creates definitions** via FastAPI endpoints (indexes, rules, labeling functions)
2. **FastAPI validates and stores** definitions in PostgreSQL
3. **User triggers materialization** via API
4. **FastAPI sends job to Dagster** via GraphQL
5. **Dagster executes SQL queries** and uploads parquet results to S3
6. **User configures pipeline** in React Flow UI
7. **User triggers training** (Snorkel or classifier)
8. **Results returned** via API (softmax probabilities or hard labels)

### Occupancy Pipeline (Independent)
- Creates per-space per-time-chunk materialization jobs
- Background sensor (`source_chunk_submission_sensor`) submits jobs every 30 seconds
- Bottom-up aggregation: source spaces (rooms) -> derived spaces (floors, buildings)
- Completed chunks stored at fixed S3 keys and reused across datasets

## Key Files for Common Tasks

### Adding a New API Endpoint
1. Create router in `backend/routers/new_router.py`
2. Register in `backend/main.py`
3. Add schemas to `backend/schemas.py`
4. Add models to `backend/db/models.py`

### Adding a New Dagster Job
1. Define job in `backend/dagster_app/jobs.py`
2. Add assets/ops in `backend/dagster_app/assets.py`
3. Register in `backend/dagster_app/definitions.py`

### Database Migrations
- New tables: Add model to `models.py`, `create_all()` picks them up on startup
- Altering existing tables: Add DDL block to `_run_migrations()` in `backend/db/session.py`

## Environment Variables

Required in `.env` (see `.env.example`):
- `DATABASE_URL` - Local PostgreSQL connection
- `TIPPERS_DB_URL` - External tippers database connection
- `S3_BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` - S3 storage
- `DAGSTER_MAX_CONCURRENT` - Max parallel Dagster workers (default: 8)
- `SOURCE_CHUNK_SENSOR_INTERVAL` - Sensor polling interval in seconds (default: 30)

## Tech Stack

**Backend**: FastAPI, SQLAlchemy, Dagster, Snorkel, MLflow, boto3, pandas, pyarrow
**Frontend**: React 19, TypeScript, Vite, @xyflow/react (React Flow), TailwindCSS, Zustand, TanStack Query
**Infrastructure**: Docker Compose, PostgreSQL, Redis, S3
