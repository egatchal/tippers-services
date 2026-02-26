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
│   │   ├── occupancy.py        # Occupancy dataset endpoints
│   │   └── dagster.py          # Dagster status endpoints
│   ├── utils/
│   │   ├── dagster_client.py   # GraphQL client for Dagster
│   │   ├── validators.py       # SQL and Python validation
│   │   └── timeout_calculator.py  # Dynamic timeout calculation
│   ├── db/
│   │   ├── session.py          # Database session management
│   │   └── models.py           # SQLAlchemy models
│   └── dagster_app/            # CRITICAL: Named 'dagster_app' to avoid module shadowing
│       ├── definitions.py      # Dagster definitions
│       ├── workspace.yaml      # Dagster workspace config
│       ├── resources.py        # Dagster resources (DB, S3, MLflow)
│       ├── jobs.py             # Dagster job definitions
│       └── assets.py           # Dagster assets and sensors
├── dagster_home/
│   └── dagster.yaml            # Dagster instance configuration
├── docker-compose.yml          # Multi-service Docker setup
├── Dockerfile                  # Backend service Docker image
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (not in git)
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
# Database Configuration
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/tippers
TIPPERS_DB_URL=postgresql://user:pass@tippers-db:5432/tippers

# AWS credentials for S3 storage
S3_BUCKET_NAME=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1

# Dagster Configuration
DAGSTER_HOST=dagster-webserver
DAGSTER_PORT=3000
DAGSTER_MAX_CONCURRENT=16  # Max concurrent workers per job

# Occupancy Sensor Configuration
SOURCE_CHUNK_SENSOR_INTERVAL=30  # Sensor runs every 30 seconds
CHUNK_TIMEOUT_MULTIPLIER=2.0
CHUNK_TIMEOUT_MIN_SECONDS=300
CHUNK_TIMEOUT_LOOKBACK_DAYS=30
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

Interactive docs are available at `http://localhost:8000/docs` once the backend is running.

### Health

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Service health check |

### Database Connections

| Method | Path | Description |
|--------|------|-------------|
| GET | `/connections` | List all connections |
| POST | `/connections` | Create a connection |
| GET | `/connections/{conn_id}` | Get a connection |
| PUT | `/connections/{conn_id}` | Update a connection |
| DELETE | `/connections/{conn_id}` | Delete a connection |
| POST | `/connections/{conn_id}/test` | Test a connection |

### Concepts & Catalog

| Method | Path | Description |
|--------|------|-------------|
| GET | `/concepts` | List concepts |
| POST | `/concepts` | Create a concept |
| GET | `/concepts/{c_id}` | Get a concept |
| PUT | `/concepts/{c_id}` | Update a concept |
| DELETE | `/concepts/{c_id}` | Delete a concept |
| GET | `/concepts/{c_id}/catalog` | Asset catalog for a concept |

### Concept Values (Labels)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/concepts/{c_id}/values` | List concept values |
| POST | `/concepts/{c_id}/values` | Create a concept value |
| PUT | `/concepts/{c_id}/values/{cv_id}` | Update a concept value |
| DELETE | `/concepts/{c_id}/values/{cv_id}` | Delete a concept value |

### Indexes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/concepts/{c_id}/indexes` | List indexes |
| POST | `/concepts/{c_id}/indexes` | Create an index |
| GET | `/concepts/{c_id}/indexes/{index_id}` | Get an index |
| PUT | `/concepts/{c_id}/indexes/{index_id}` | Update an index |
| DELETE | `/concepts/{c_id}/indexes/{index_id}` | Delete an index |
| POST | `/concepts/{c_id}/indexes/{index_id}/materialize` | Trigger materialization |
| GET | `/concepts/{c_id}/indexes/{index_id}/preview` | Preview index data |

### Rules

| Method | Path | Description |
|--------|------|-------------|
| GET | `/concepts/{c_id}/rules` | List rules |
| POST | `/concepts/{c_id}/rules` | Create a rule |
| GET | `/concepts/{c_id}/rules/{r_id}` | Get a rule |
| PUT | `/concepts/{c_id}/rules/{r_id}` | Update a rule |
| DELETE | `/concepts/{c_id}/rules/{r_id}` | Delete a rule |
| POST | `/concepts/{c_id}/rules/{r_id}/materialize` | Trigger materialization |
| GET | `/concepts/{c_id}/rules/{r_id}/preview` | Preview rule data |

### Labeling Functions

| Method | Path | Description |
|--------|------|-------------|
| GET | `/concepts/{c_id}/labeling-functions` | List labeling functions |
| POST | `/concepts/{c_id}/labeling-functions` | Create a labeling function |
| GET | `/concepts/{c_id}/labeling-functions/{lf_id}` | Get a labeling function |
| PUT | `/concepts/{c_id}/labeling-functions/{lf_id}` | Update a labeling function |
| DELETE | `/concepts/{c_id}/labeling-functions/{lf_id}` | Delete a labeling function |

### Snorkel Training

| Method | Path | Description |
|--------|------|-------------|
| POST | `/concepts/{c_id}/snorkel/run` | Trigger Snorkel training |
| GET | `/concepts/{c_id}/snorkel/jobs` | List Snorkel jobs |
| GET | `/concepts/{c_id}/snorkel/jobs/{job_id}` | Get job status |
| GET | `/concepts/{c_id}/snorkel/jobs/{job_id}/results` | Get training results |

### Features

| Method | Path | Description |
|--------|------|-------------|
| GET | `/concepts/{c_id}/features` | List features |
| POST | `/concepts/{c_id}/features` | Create a feature |
| GET | `/concepts/{c_id}/features/{feature_id}` | Get a feature |
| PUT | `/concepts/{c_id}/features/{feature_id}` | Update a feature |
| DELETE | `/concepts/{c_id}/features/{feature_id}` | Delete a feature |
| POST | `/concepts/{c_id}/features/{feature_id}/materialize` | Trigger materialization |

### Classifiers

| Method | Path | Description |
|--------|------|-------------|
| POST | `/concepts/{c_id}/classifiers/run` | Trigger classifier training |
| GET | `/concepts/{c_id}/classifiers/jobs` | List classifier jobs |
| GET | `/concepts/{c_id}/classifiers/jobs/{job_id}` | Get job status |
| GET | `/concepts/{c_id}/classifiers/jobs/{job_id}/results` | Get classifier results |

### Occupancy Datasets

Compute WiFi-session-based occupancy counts over a space subtree using **bottom-up per-space chunk materialization**. The API returns **instantly** (<1 second); computation is handled asynchronously by background sensors. Jobs are split into one Dagster run per source space per time chunk, with derived spaces (floors, buildings) triggered automatically once their children complete. Completed chunks are stored at a fixed S3 key and reused across datasets — extending a time range only computes the new chunks.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/occupancy/datasets` | Create dataset and submit chunk jobs |
| GET | `/occupancy/datasets` | List all datasets |
| GET | `/occupancy/datasets/{dataset_id}` | Get dataset status |
| GET | `/occupancy/datasets/{dataset_id}/results?space_id=X` | Get results for one space (progress or data) |
| DELETE | `/occupancy/datasets/{dataset_id}` | Delete a dataset record |

**POST `/occupancy/datasets` body:**
```json
{
  "name": "Engineering Hall – Q1 2025",
  "description": "Optional",
  "root_space_id": 1234,
  "start_time": "2025-01-01T00:00:00Z",
  "end_time": "2025-04-01T00:00:00Z",
  "interval_seconds": 3600,
  "chunk_days": 30
}
```

**GET `/occupancy/datasets/{id}/results?space_id=X`** — `space_id` defaults to `root_space_id`. Returns `{"status": "RUNNING", "completed_chunks": N, "total_chunks": M}` while computing, or concatenated parquet rows once all chunks for that space are COMPLETED.

### Spaces (read-only, tippers external DB)

Browse the `space` table on the external tippers DB to look up valid `root_space_id` values.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/spaces` | List spaces (`?search=name&skip=0&limit=100`) |
| GET | `/spaces/{space_id}` | Get a single space (404 if not found) |
| GET | `/spaces/{space_id}/children` | Direct children of a space |
| GET | `/spaces/{space_id}/subtree` | All descendants including root (recursive CTE) |

**Space response fields:** `space_id`, `space_name`, `parent_space_id`, `building_room`

### Models (MLflow)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/models` | List hosted models |
| POST | `/models` | Register a model |
| GET | `/models/{model_id}` | Get a model |
| DELETE | `/models/{model_id}` | Delete a model |

### Dagster

| Method | Path | Description |
|--------|------|-------------|
| GET | `/dagster/runs/{run_id}` | Check Dagster run status |

## Applying Code Changes

### Docker (normal workflow)

Restart only the affected services after editing Python files:

```bash
# Restart FastAPI and both Dagster services (picks up router/schema/asset changes)
docker-compose restart fastapi dagster-webserver dagster-daemon
```

If you added a new Python file or changed `requirements.txt`, you need to rebuild first:

```bash
# Rebuild image then restart
docker-compose up -d --build fastapi dagster-webserver dagster-daemon
```

To restart everything from scratch (useful when the DB schema changed):

```bash
docker-compose down && docker-compose up -d
```

> **New DB table?** `create_all()` picks up new models automatically on startup. Altering an existing table requires adding a DDL block to `_run_migrations()` in `backend/db/session.py`.

### Local development (without Docker)

```bash
# Terminal 1 — FastAPI with auto-reload
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000

# Terminal 2 — Dagster
dagster dev -w backend/dagster_app/workspace.yaml
```

`--reload` makes uvicorn watch for file changes and restart automatically, so no manual restart is needed for FastAPI. Dagster also hot-reloads code locations in `dagster dev` mode.

---

## Configuration

### Dagster Concurrent Run Limits

The system uses Dagster's `QueuedRunCoordinator` to manage concurrent job execution. By default, up to **16 concurrent runs** can execute simultaneously.

**To adjust the concurrent run limit:**

1. Edit `dagster_home/dagster.yaml`:

```yaml
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 16  # Change this value
```

2. Restart Dagster services:

```bash
docker-compose restart dagster-daemon dagster-webserver
```

**How it works:**

- The `source_chunk_submission_sensor` submits jobs every 30 seconds (configurable via `SOURCE_CHUNK_SENSOR_INTERVAL`)
- Dagster's run coordinator queues submitted jobs and launches up to `max_concurrent_runs` simultaneously
- Each job can spawn multiple worker processes (controlled by `DAGSTER_MAX_CONCURRENT`)
- Total parallelism = `max_concurrent_runs` × `DAGSTER_MAX_CONCURRENT` workers

**Performance considerations:**

- Higher `max_concurrent_runs` = more parallel jobs but higher memory/CPU usage
- Monitor system resources and database connection pool limits
- The default connection pool size is 50 connections (see `backend/db/session.py`)

---

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

**Occupancy pipeline (independent of concept pipeline):**

1. `POST /occupancy/datasets` **returns instantly** (<1 second) after creating dataset and chunk records. It walks the space subtree from tippers DB, finds source spaces (rooms with WiFi data), and creates one `OccupancySpaceChunk` record per (space, epoch-aligned chunk window) combination
2. A **background sensor** (`source_chunk_submission_sensor`) runs every 30 seconds, finds PENDING source chunks with no dagster_run_id, and submits up to 100 jobs per iteration
3. **Dagster jobs** (`materialize_source_chunk_job`) run in parallel (up to 16 concurrent runs by default), each writing sparse parquet to `occupancy/spaces/{space_id}/{interval}/{chunk_start}_{chunk_end}.parquet`
4. A **Dagster sensor** (`occupancy_space_chunk_sensor`) watches for completed source chunks and triggers derived chunk jobs (floors → buildings) bottom-up
5. `GET /results?space_id=X` polls chunk status and streams concatenated parquet rows once all chunks complete

### Key Design Decisions

- **dagster_app directory name**: Prevents module shadowing issues with the `dagster` package
- **PYTHONPATH=/app**: Ensures imports work correctly in Docker
- **GraphQL client**: FastAPI communicates with Dagster via GraphQL API
- **Asset-based design**: Indexes and Rules are materialized, reusable Dagster assets
- **Softmax output**: Optional probabilistic predictions from Snorkel
- **Chunk reuse**: Occupancy chunks are keyed by `(space_id, interval_seconds, chunk_start, chunk_end)` — a COMPLETED chunk is never re-run, even if requested by a different dataset covering the same window
- **Sparse parquet**: Only bins with at least one connection are stored; parent aggregation treats absent bins as 0

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
