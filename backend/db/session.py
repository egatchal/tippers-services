from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import os
import logging
import time

logger = logging.getLogger(__name__)

# Get database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/tippers")

# Create engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


def get_db():
    """Dependency to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database tables with retry logic for connection."""
    max_retries = 5
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            # Import models to register them with Base
            from backend.db import models

            # Create all tables
            logger.info(f"Attempting to create database tables (attempt {attempt + 1}/{max_retries})...")
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables created successfully.")
            return
        except OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database not ready yet, retrying in {retry_delay}s... (Error: {e})")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to database after {max_retries} attempts")
                raise
        except Exception as e:
            logger.error(f"Unexpected error initializing database: {e}")
            raise
