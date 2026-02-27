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
        # ---------------------------------------------------------------
        # Phase 1: Unified Dataset Catalog
        # ---------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS datasets (
            dataset_id   SERIAL PRIMARY KEY,
            name         VARCHAR(255) NOT NULL,
            description  TEXT,
            service      VARCHAR(64) NOT NULL,
            dataset_type VARCHAR(64) NOT NULL,
            format       VARCHAR(32) DEFAULT 'parquet',
            storage_path VARCHAR(500),
            source_ref   JSONB,
            row_count    INTEGER,
            column_stats JSONB,
            schema_info  JSONB,
            tags         JSONB,
            status       VARCHAR(50) DEFAULT 'AVAILABLE',
            created_at   TIMESTAMP DEFAULT NOW(),
            updated_at   TIMESTAMP DEFAULT NOW()
        )
        """,
        # Ensure JSONB columns on datasets (fixes tables created by create_all with JSON type)
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'datasets' AND column_name = 'source_ref' AND data_type = 'json'
            ) THEN
                ALTER TABLE datasets
                    ALTER COLUMN source_ref   TYPE JSONB USING source_ref::jsonb,
                    ALTER COLUMN column_stats TYPE JSONB USING column_stats::jsonb,
                    ALTER COLUMN schema_info  TYPE JSONB USING schema_info::jsonb,
                    ALTER COLUMN tags         TYPE JSONB USING tags::jsonb;
            END IF;
        END $$
        """,
        # Ensure JSONB columns on jobs table
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'jobs' AND column_name = 'service_job_ref' AND data_type = 'json'
            ) THEN
                ALTER TABLE jobs
                    ALTER COLUMN service_job_ref TYPE JSONB USING service_job_ref::jsonb,
                    ALTER COLUMN config         TYPE JSONB USING config::jsonb;
            END IF;
        END $$
        """,
        # Backfill datasets from existing materialized concept_indexes
        """
        INSERT INTO datasets (name, service, dataset_type, format, storage_path, row_count, column_stats, source_ref, tags, status, created_at, updated_at)
        SELECT
            ci.name,
            'snorkel',
            'index',
            'parquet',
            ci.storage_path,
            ci.row_count,
            ci.column_stats::jsonb,
            jsonb_build_object('entity_type', 'concept_index', 'entity_id', ci.index_id),
            jsonb_build_object('concept_id', ci.c_id),
            'AVAILABLE',
            ci.materialized_at,
            ci.materialized_at
        FROM concept_indexes ci
        WHERE ci.storage_path IS NOT NULL
          AND ci.is_materialized = true
          AND NOT EXISTS (
              SELECT 1 FROM datasets d
              WHERE d.source_ref @> jsonb_build_object('entity_type', 'concept_index', 'entity_id', ci.index_id)
          )
        """,
        # Backfill datasets from existing materialized concept_rules
        """
        INSERT INTO datasets (name, service, dataset_type, format, storage_path, row_count, column_stats, source_ref, tags, status, created_at, updated_at)
        SELECT
            cr.name,
            'snorkel',
            'rule_features',
            'parquet',
            cr.storage_path,
            cr.row_count,
            cr.column_stats::jsonb,
            jsonb_build_object('entity_type', 'concept_rule', 'entity_id', cr.r_id),
            jsonb_build_object('concept_id', cr.c_id),
            'AVAILABLE',
            cr.materialized_at,
            cr.materialized_at
        FROM concept_rules cr
        WHERE cr.storage_path IS NOT NULL
          AND cr.is_materialized = true
          AND NOT EXISTS (
              SELECT 1 FROM datasets d
              WHERE d.source_ref @> jsonb_build_object('entity_type', 'concept_rule', 'entity_id', cr.r_id)
          )
        """,
        # Backfill datasets from existing materialized concept_features
        """
        INSERT INTO datasets (name, service, dataset_type, format, storage_path, row_count, column_stats, source_ref, tags, status, created_at, updated_at)
        SELECT
            cf.name,
            'snorkel',
            'features',
            'parquet',
            cf.storage_path,
            cf.row_count,
            cf.column_stats::jsonb,
            jsonb_build_object('entity_type', 'concept_feature', 'entity_id', cf.feature_id),
            jsonb_build_object('concept_id', cf.c_id),
            'AVAILABLE',
            cf.materialized_at,
            cf.materialized_at
        FROM concept_features cf
        WHERE cf.storage_path IS NOT NULL
          AND cf.is_materialized = true
          AND NOT EXISTS (
              SELECT 1 FROM datasets d
              WHERE d.source_ref @> jsonb_build_object('entity_type', 'concept_feature', 'entity_id', cf.feature_id)
          )
        """,
        # Backfill datasets from existing occupancy_datasets (only on pre-parent_dataset_id DBs)
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'occupancy_datasets' AND column_name = 'parent_dataset_id'
            ) THEN
                INSERT INTO datasets (name, description, service, dataset_type, format, storage_path, row_count, column_stats, source_ref, tags, status, created_at, updated_at)
                SELECT
                    od.name,
                    od.description,
                    'occupancy',
                    'occupancy_timeseries',
                    'parquet',
                    od.storage_path,
                    od.row_count,
                    od.column_stats::jsonb,
                    jsonb_build_object('entity_type', 'occupancy_dataset', 'entity_id', od.dataset_id),
                    jsonb_build_object('space_id', od.root_space_id),
                    'AVAILABLE',
                    od.created_at,
                    COALESCE(od.completed_at, od.created_at)
                FROM occupancy_datasets od
                WHERE od.storage_path IS NOT NULL
                  AND od.status = 'COMPLETED'
                  AND NOT EXISTS (
                      SELECT 1 FROM datasets d
                      WHERE d.source_ref @> jsonb_build_object('entity_type', 'occupancy_dataset', 'entity_id', od.dataset_id)
                  );
            END IF;
        END $$
        """,
        # Backfill datasets from completed per-space occupancy chunks (only on pre-parent_dataset_id DBs)
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'occupancy_space_chunks' AND column_name = 'parent_dataset_id'
            ) THEN
                INSERT INTO datasets (name, service, dataset_type, format, storage_path, source_ref, tags, status, created_at, updated_at)
                SELECT
                    'Occupancy space ' || c.space_id || ' (' || c.interval_seconds || 's)',
                    'occupancy',
                    'occupancy_space_chunk',
                    'parquet',
                    c.storage_path,
                    jsonb_build_object('entity_type', 'occupancy_space_chunk', 'space_id', c.space_id, 'interval_seconds', c.interval_seconds),
                    jsonb_build_object('space_id', c.space_id, 'interval_seconds', c.interval_seconds, 'space_type', c.space_type),
                    'AVAILABLE',
                    c.completed_at,
                    c.completed_at
                FROM occupancy_space_chunks c
                WHERE c.status = 'COMPLETED'
                  AND c.storage_path IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM datasets d
                      WHERE d.source_ref @> jsonb_build_object('entity_type', 'occupancy_space_chunk', 'space_id', c.space_id, 'interval_seconds', c.interval_seconds)
                        AND d.storage_path = c.storage_path
                  );
            END IF;
        END $$
        """,
        # ---------------------------------------------------------------
        # Phase 2: Unified Job Tracker
        # ---------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id             SERIAL PRIMARY KEY,
            service            VARCHAR(64) NOT NULL,
            job_type           VARCHAR(64) NOT NULL,
            service_job_ref    JSONB,
            dagster_run_id     VARCHAR(255),
            dagster_job_name   VARCHAR(255),
            config             JSONB,
            status             VARCHAR(50) DEFAULT 'PENDING',
            error_message      TEXT,
            input_dataset_ids  INTEGER[],
            output_dataset_ids INTEGER[],
            created_at         TIMESTAMP DEFAULT NOW(),
            started_at         TIMESTAMP,
            completed_at       TIMESTAMP
        )
        """,
        # Add unified_job_id FK columns to existing job tables
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'snorkel_jobs' AND column_name = 'unified_job_id'
            ) THEN
                ALTER TABLE snorkel_jobs ADD COLUMN unified_job_id INTEGER REFERENCES jobs(job_id);
            END IF;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'classifier_jobs' AND column_name = 'unified_job_id'
            ) THEN
                ALTER TABLE classifier_jobs ADD COLUMN unified_job_id INTEGER REFERENCES jobs(job_id);
            END IF;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'occupancy_datasets' AND column_name = 'unified_job_id'
            ) THEN
                ALTER TABLE occupancy_datasets ADD COLUMN unified_job_id INTEGER REFERENCES jobs(job_id);
            END IF;
        END $$;
        """,
        # ---------------------------------------------------------------
        # Phase 3: Service Deployments
        # ---------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS service_deployments (
            deployment_id    SERIAL PRIMARY KEY,
            model_id         INTEGER REFERENCES hosted_models(model_id),
            model_version_id INTEGER REFERENCES hosted_model_versions(model_version_id),
            service          VARCHAR(64) NOT NULL,
            status           VARCHAR(32) DEFAULT 'ACTIVE',
            deploy_config    JSONB,
            mlflow_model_uri VARCHAR(500),
            created_at       TIMESTAMP DEFAULT NOW(),
            updated_at       TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_service_deployment_service_model UNIQUE (service, model_id)
        )
        """,
        # ---------------------------------------------------------------
        # Phase 4: Workflow Engine
        # ---------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS workflow_templates (
            template_id SERIAL PRIMARY KEY,
            name        VARCHAR(255) NOT NULL,
            service     VARCHAR(64) NOT NULL,
            description TEXT,
            steps       JSONB NOT NULL,
            is_active   BOOLEAN DEFAULT true,
            created_at  TIMESTAMP DEFAULT NOW(),
            updated_at  TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_workflow_template_service_name UNIQUE (service, name)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS workflow_runs (
            run_id        SERIAL PRIMARY KEY,
            template_id   INTEGER REFERENCES workflow_templates(template_id),
            service       VARCHAR(64) NOT NULL,
            params        JSONB,
            step_statuses JSONB,
            status        VARCHAR(50) DEFAULT 'PENDING',
            error_message TEXT,
            created_at    TIMESTAMP DEFAULT NOW(),
            completed_at  TIMESTAMP
        )
        """,
        # Seed workflow templates (idempotent via ON CONFLICT)
        """
        INSERT INTO workflow_templates (name, service, description, steps)
        VALUES (
            'Snorkel Full Pipeline',
            'snorkel',
            'Full weak supervision pipeline: materialize index -> materialize rules -> Snorkel training -> classifier training',
            '{"steps": [
                {"key": "materialize_index", "dagster_job": "materialize_index_job",
                 "config_template": {"ops": {"materialized_index": {"config": {"index_id": "{{index_id}}"}}}},
                 "depends_on": []},
                {"key": "materialize_rules", "dagster_job": "materialize_rule_job",
                 "config_template": {"ops": {"materialized_rule": {"config": {"rule_id": "{{rule_id}}"}}}},
                 "depends_on": ["materialize_index"]},
                {"key": "snorkel_train", "dagster_job": "snorkel_training_pipeline",
                 "config_template": {"ops": {"snorkel_training": {"config": {"job_id": "{{snorkel_job_id}}"}}}},
                 "depends_on": ["materialize_rules"]},
                {"key": "classifier_train", "dagster_job": "classifier_training_pipeline",
                 "config_template": {"ops": {"classifier_training": {"config": {"job_id": "{{classifier_job_id}}"}}}},
                 "depends_on": ["snorkel_train"]}
            ]}'::jsonb
        )
        ON CONFLICT (service, name) DO NOTHING
        """,
        """
        INSERT INTO workflow_templates (name, service, description, steps)
        VALUES (
            'Occupancy Dataset',
            'occupancy',
            'Create occupancy dataset from WiFi session data',
            '{"steps": [
                {"key": "create_dataset", "dagster_job": "occupancy_dataset_job",
                 "config_template": {"ops": {"plan_occupancy_chunks": {"config": {"dataset_id": "{{dataset_id}}"}}}},
                 "depends_on": []}
            ]}'::jsonb
        )
        ON CONFLICT (service, name) DO NOTHING
        """,
        # ---------------------------------------------------------------
        # Stale record cleanup (runs every startup)
        # ---------------------------------------------------------------
        # Auto-clean stale occupancy_space_chunks older than 24 hours
        """
        DELETE FROM occupancy_space_chunks
        WHERE status IN ('PENDING', 'FAILED', 'RUNNING')
          AND created_at < NOW() - INTERVAL '24 hours'
        """,
        # Mark stale jobs as FAILED
        """
        UPDATE jobs
        SET status = 'FAILED', error_message = 'Auto-cleaned: stale record'
        WHERE status IN ('PENDING', 'RUNNING')
          AND created_at < NOW() - INTERVAL '24 hours'
        """,
        # Mark stale occupancy_datasets as FAILED
        """
        UPDATE occupancy_datasets
        SET status = 'FAILED', error_message = 'Auto-cleaned: stale record'
        WHERE status IN ('PENDING', 'RUNNING')
          AND created_at < NOW() - INTERVAL '24 hours'
        """,
        # ---------------------------------------------------------------
        # Consolidate occupancy_datasets under datasets (parent FK)
        # ---------------------------------------------------------------
        # Add parent_dataset_id FK to occupancy_datasets
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'occupancy_datasets' AND column_name = 'parent_dataset_id'
            ) THEN
                ALTER TABLE occupancy_datasets
                ADD COLUMN parent_dataset_id INTEGER REFERENCES datasets(dataset_id);
            END IF;
        END $$;
        """,
        # Add parent_dataset_id FK to occupancy_space_chunks
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'occupancy_space_chunks' AND column_name = 'parent_dataset_id'
            ) THEN
                ALTER TABLE occupancy_space_chunks
                ADD COLUMN parent_dataset_id INTEGER REFERENCES datasets(dataset_id);
            END IF;
        END $$;
        """,
        # Backfill: link completed occupancy_datasets to their matching datasets rows
        """
        UPDATE occupancy_datasets od
        SET parent_dataset_id = d.dataset_id
        FROM datasets d
        WHERE od.parent_dataset_id IS NULL
          AND od.status = 'COMPLETED'
          AND d.source_ref @> jsonb_build_object('entity_type', 'occupancy_dataset', 'entity_id', od.dataset_id)
        """,
        # Backfill: link completed occupancy_space_chunks to their matching datasets rows
        """
        UPDATE occupancy_space_chunks c
        SET parent_dataset_id = d.dataset_id
        FROM datasets d
        WHERE c.parent_dataset_id IS NULL
          AND c.status = 'COMPLETED'
          AND d.source_ref @> jsonb_build_object('entity_type', 'occupancy_space_chunk', 'space_id', c.space_id, 'interval_seconds', c.interval_seconds)
          AND d.storage_path = c.storage_path
        """,
        # ---------------------------------------------------------------
        # Hierarchical Snorkel Pipeline Migrations
        # ---------------------------------------------------------------
        # Add parent_cv_id to concept_values for CV tree hierarchy
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='concept_values' AND column_name='parent_cv_id')
            THEN ALTER TABLE concept_values ADD COLUMN parent_cv_id INTEGER REFERENCES concept_values(cv_id);
            END IF;
        END $$;
        """,
        # Add source_type to concept_indexes
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='concept_indexes' AND column_name='source_type')
            THEN ALTER TABLE concept_indexes ADD COLUMN source_type VARCHAR(20) NOT NULL DEFAULT 'sql';
            END IF;
        END $$;
        """,
        # Add cv_id to concept_indexes
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='concept_indexes' AND column_name='cv_id')
            THEN ALTER TABLE concept_indexes ADD COLUMN cv_id INTEGER REFERENCES concept_values(cv_id);
            END IF;
        END $$;
        """,
        # Add parent_index_id to concept_indexes
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='concept_indexes' AND column_name='parent_index_id')
            THEN ALTER TABLE concept_indexes ADD COLUMN parent_index_id INTEGER REFERENCES concept_indexes(index_id);
            END IF;
        END $$;
        """,
        # Add parent_snorkel_job_id, label_filter, filtered_count to concept_indexes
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='concept_indexes' AND column_name='parent_snorkel_job_id')
            THEN
                ALTER TABLE concept_indexes ADD COLUMN parent_snorkel_job_id INTEGER REFERENCES snorkel_jobs(job_id);
                ALTER TABLE concept_indexes ADD COLUMN label_filter JSONB;
                ALTER TABLE concept_indexes ADD COLUMN filtered_count INTEGER;
            END IF;
        END $$;
        """,
        # Make sql_query nullable on concept_indexes (derived indexes have no SQL)
        """
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='concept_indexes' AND column_name='sql_query' AND is_nullable='NO')
            THEN ALTER TABLE concept_indexes ALTER COLUMN sql_query DROP NOT NULL;
            END IF;
        END $$;
        """,
        # Add JSONB metadata columns to snorkel_jobs
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='snorkel_jobs' AND column_name='lf_summary')
            THEN
                ALTER TABLE snorkel_jobs ADD COLUMN lf_summary JSONB;
                ALTER TABLE snorkel_jobs ADD COLUMN class_distribution JSONB;
                ALTER TABLE snorkel_jobs ADD COLUMN overall_stats JSONB;
                ALTER TABLE snorkel_jobs ADD COLUMN cv_id_to_index JSONB;
                ALTER TABLE snorkel_jobs ADD COLUMN cv_id_to_name JSONB;
            END IF;
        END $$;
        """,
        # Make encrypted_password nullable on database_connections (password not always required)
        """
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='database_connections' AND column_name='encrypted_password' AND is_nullable='NO')
            THEN ALTER TABLE database_connections ALTER COLUMN encrypted_password DROP NOT NULL;
            END IF;
        END $$;
        """,
        # Add key_column to concept_indexes (identifies the unique entity column)
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='concept_indexes' AND column_name='key_column')
            THEN ALTER TABLE concept_indexes ADD COLUMN key_column VARCHAR(255);
            END IF;
        END $$;
        """,
        # Make rule_id nullable on labeling_functions (rule set via canvas edge)
        """
        ALTER TABLE labeling_functions ALTER COLUMN rule_id DROP NOT NULL;
        """,
        # Make index_id nullable on snorkel_jobs (index set via canvas edge for draft jobs)
        """
        ALTER TABLE snorkel_jobs ALTER COLUMN index_id DROP NOT NULL;
        """,
        # Add output_type to concept_indexes (for derived indexes: softmax or hard_labels)
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name='concept_indexes' AND column_name='output_type')
            THEN ALTER TABLE concept_indexes ADD COLUMN output_type VARCHAR(50);
            END IF;
        END $$;
        """,
    ]

    for i, sql in enumerate(migrations):
        try:
            with engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except Exception as e:
            logger.warning(f"Schema migration step {i} warning: {e}")
