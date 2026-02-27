"""Test fixtures for the Tippers Services integration tests.

These tests run against a real PostgreSQL database (same as dev) via
the FastAPI TestClient. The Docker stack must be running:

    docker-compose up -d postgres fastapi

Set TEST_DATABASE_URL if you want a separate test database:

    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/tippers_test
"""
import os
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

# Point at test DB if configured, otherwise use the dev DB
if os.getenv("TEST_DATABASE_URL"):
    os.environ["DATABASE_URL"] = os.getenv("TEST_DATABASE_URL")

from backend.main import app
from backend.db.session import get_db, engine, SessionLocal, Base


@pytest.fixture(scope="session", autouse=True)
def init_test_db():
    """Ensure all tables exist before tests run."""
    from backend.db import models  # noqa: F401 — registers models with Base
    Base.metadata.create_all(bind=engine)
    yield


@pytest.fixture()
def db():
    """Provide a transactional DB session that rolls back after each test."""
    connection = engine.connect()
    transaction = connection.begin()
    session = SessionLocal(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture()
def client(db):
    """FastAPI TestClient wired to the transactional test session."""
    def _override_get_db():
        try:
            yield db
        finally:
            pass  # session lifecycle managed by the db fixture

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture()
def mock_dagster():
    """Mock the Dagster client so materialization triggers don't need Dagster running."""
    mock_client = MagicMock()
    mock_client.submit_job_execution.return_value = {
        "run_id": "test-run-mock-001",
        "status": "LAUNCHED",
    }

    with patch("backend.utils.dagster_client.get_dagster_client", return_value=mock_client):
        yield mock_client
