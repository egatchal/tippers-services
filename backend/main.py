from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.db.session import init_db
from backend.routers import (
    catalog,
    indexes,
    rules,
    labeling_functions,
    snorkel,
    dagster,
    database_connections
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Tippers Services API",
    description="FastAPI + Dagster Integration for Data Workflows",
    version="1.0.0"
)


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup."""
    logger.info("Starting up: Initializing database...")
    try:
        init_db()
        logger.info("Database initialization completed successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(database_connections.router, tags=["database_connections"])
app.include_router(catalog.router, prefix="/concepts", tags=["catalog"])
app.include_router(indexes.router, prefix="/concepts", tags=["indexes"])
app.include_router(rules.router, prefix="/concepts", tags=["rules"])
app.include_router(labeling_functions.router, prefix="/concepts", tags=["labeling_functions"])
app.include_router(snorkel.router, prefix="/concepts", tags=["snorkel"])
app.include_router(dagster.router, prefix="/dagster", tags=["dagster"])

@app.get("/")
async def root():
    return {"message": "Tippers Services API - FastAPI + Dagster Integration"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
