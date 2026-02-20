from sqlalchemy import create_engine, text
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

            # Run schema migrations for existing tables
            _run_migrations(engine)
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


def _run_migrations(engine):
    """Run idempotent schema migrations for existing tables."""
    migrations = [
        # Add applicable_cv_ids to labeling_functions (replaces old cv_id column)
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'labeling_functions' AND column_name = 'applicable_cv_ids'
            ) THEN
                ALTER TABLE labeling_functions
                ADD COLUMN applicable_cv_ids INTEGER[] NOT NULL DEFAULT '{}';
            END IF;
        END $$;
        """,
        # Migrate cv_id data to applicable_cv_ids, then drop cv_id
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'labeling_functions' AND column_name = 'cv_id'
            ) THEN
                UPDATE labeling_functions
                SET applicable_cv_ids = ARRAY[cv_id]
                WHERE cv_id IS NOT NULL AND applicable_cv_ids = '{}';
                ALTER TABLE labeling_functions DROP COLUMN cv_id;
            END IF;
        END $$;
        """,
        # Add rule_id to labeling_functions if not exists
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'labeling_functions' AND column_name = 'rule_id'
            ) THEN
                ALTER TABLE labeling_functions
                ADD COLUMN rule_id INTEGER REFERENCES concept_rules(r_id);
            END IF;
        END $$;
        """,
        # Drop applicable_cv_ids from concept_rules if exists (moved to LFs)
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'concept_rules' AND column_name = 'applicable_cv_ids'
            ) THEN
                ALTER TABLE concept_rules DROP COLUMN applicable_cv_ids;
            END IF;
        END $$;
        """,
        # Drop label_guidance from concept_rules if exists
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'concept_rules' AND column_name = 'label_guidance'
            ) THEN
                ALTER TABLE concept_rules DROP COLUMN label_guidance;
            END IF;
        END $$;
        """,
        # Add column_stats JSONB to concept_indexes
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'concept_indexes' AND column_name = 'column_stats'
            ) THEN
                ALTER TABLE concept_indexes ADD COLUMN column_stats JSONB;
            END IF;
        END $$;
        """,
        # Add column_stats JSONB to concept_rules
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'concept_rules' AND column_name = 'column_stats'
            ) THEN
                ALTER TABLE concept_rules ADD COLUMN column_stats JSONB;
            END IF;
        END $$;
        """,
        # Add column_stats JSONB to concept_features
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'concept_features' AND column_name = 'column_stats'
            ) THEN
                ALTER TABLE concept_features ADD COLUMN column_stats JSONB;
            END IF;
        END $$;
        """,
        # Add level to concept_values
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'concept_values' AND column_name = 'level'
            ) THEN
                ALTER TABLE concept_values ADD COLUMN level INTEGER NOT NULL DEFAULT 1;
            END IF;
        END $$;
        """,
        # Create occupancy_space_chunks table (idempotent)
        """
        CREATE TABLE IF NOT EXISTS occupancy_space_chunks (
            chunk_id         SERIAL PRIMARY KEY,
            space_id         INTEGER NOT NULL,
            interval_seconds INTEGER NOT NULL,
            chunk_start      TIMESTAMP NOT NULL,
            chunk_end        TIMESTAMP NOT NULL,
            space_type       VARCHAR(16) NOT NULL,
            status           VARCHAR(16) NOT NULL DEFAULT 'PENDING',
            storage_path     VARCHAR(500),
            dagster_run_id   VARCHAR(255),
            error_message    TEXT,
            created_at       TIMESTAMP DEFAULT NOW(),
            completed_at     TIMESTAMP,
            CONSTRAINT uq_occupancy_space_chunk
                UNIQUE (space_id, interval_seconds, chunk_start, chunk_end)
        )
        """,
    ]

    try:
        with engine.connect() as conn:
            for sql in migrations:
                conn.execute(text(sql))
            conn.commit()
        logger.info("Schema migrations completed successfully.")
    except Exception as e:
        logger.warning(f"Schema migration warning: {e}")
