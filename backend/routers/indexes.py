"""Index CRUD and materialization endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import ConceptIndex, ConceptValue, DatabaseConnection, SnorkelJob
from backend.schemas import (
    IndexCreate,
    IndexUpdate,
    IndexResponse,
    IndexMaterializeResponse,
    DerivedIndexCreate,
    LabelFilterUpdate,
    EntityPreviewResponse,
)

router = APIRouter()


@router.post("/{c_id}/indexes", response_model=IndexResponse, status_code=status.HTTP_201_CREATED)
async def create_index(
    c_id: int,
    request: IndexCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new SQL index definition.

    - **c_id**: Concept ID
    - **name**: Index name (must be unique within concept)
    - **conn_id**: Database connection ID
    - **sql_query**: SQL query to execute (supports Jinja2 templates)
    - **query_template_params**: Optional template parameters
    - **partition_type**: Optional partitioning (time, id_range, categorical)
    - **partition_config**: Partition configuration
    """
    # SQL indexes require conn_id and sql_query
    if not request.conn_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="conn_id is required for SQL indexes"
        )
    if not request.sql_query:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="sql_query is required for SQL indexes"
        )

    # Validate SQL syntax (basic validation)
    query_upper = request.sql_query.strip().upper()
    if not (query_upper.startswith("SELECT") or query_upper.startswith("WITH")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="SQL query must be a SELECT statement or CTE (WITH clause)"
        )

    # Check if name already exists for this concept
    existing = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.name == request.name
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Index with name '{request.name}' already exists for concept {c_id}"
        )

    # Verify database connection exists
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.conn_id == request.conn_id
    ).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Database connection with ID {request.conn_id} not found"
        )

    # Validate cv_id if provided: must belong to this concept and be a root CV
    if request.cv_id is not None:
        cv = db.query(ConceptValue).filter(
            ConceptValue.cv_id == request.cv_id,
            ConceptValue.c_id == c_id
        ).first()
        if not cv:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Concept value {request.cv_id} not found in concept {c_id}"
            )
        if cv.parent_cv_id is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="SQL indexes can only be assigned to root concept values (parent_cv_id must be null)"
            )

    # Create index
    index = ConceptIndex(
        c_id=c_id,
        name=request.name,
        conn_id=request.conn_id,
        key_column=request.key_column,
        sql_query=request.sql_query,
        query_template_params=request.query_template_params,
        partition_type=request.partition_type,
        partition_config=request.partition_config,
        source_type='sql',
        cv_id=request.cv_id,
        is_materialized=False
    )

    db.add(index)
    db.commit()
    db.refresh(index)

    return index


@router.get("/{c_id}/indexes", response_model=List[IndexResponse])
async def list_indexes(
    c_id: int,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List all indexes for a concept.

    - **c_id**: Concept ID
    - **skip**: Number of records to skip (pagination)
    - **limit**: Maximum number of records to return
    """
    indexes = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id
    ).offset(skip).limit(limit).all()

    return indexes


@router.get("/{c_id}/indexes/{index_id}", response_model=IndexResponse)
async def get_index(
    c_id: int,
    index_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific index by ID.

    - **c_id**: Concept ID
    - **index_id**: Index ID
    """
    index = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.index_id == index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {index_id} not found for concept {c_id}"
        )

    return index


@router.patch("/{c_id}/indexes/{index_id}", response_model=IndexResponse)
async def update_index(
    c_id: int,
    index_id: int,
    request: IndexUpdate,
    db: Session = Depends(get_db)
):
    """
    Update an index definition.

    - **c_id**: Concept ID
    - **index_id**: Index ID
    - Only provided fields will be updated
    """
    index = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.index_id == index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {index_id} not found for concept {c_id}"
        )

    # Get update data
    update_data = request.model_dump(exclude_unset=True)

    # Validate SQL if provided
    if "sql_query" in update_data:
        query_upper = update_data["sql_query"].strip().upper()
        if not (query_upper.startswith("SELECT") or query_upper.startswith("WITH")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="SQL query must be a SELECT statement or CTE (WITH clause)"
            )

    # Check name uniqueness if updating name
    if "name" in update_data:
        existing = db.query(ConceptIndex).filter(
            ConceptIndex.c_id == c_id,
            ConceptIndex.name == update_data["name"],
            ConceptIndex.index_id != index_id
        ).first()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Index with name '{update_data['name']}' already exists for concept {c_id}"
            )

    # Verify connection exists if updating conn_id
    if "conn_id" in update_data:
        connection = db.query(DatabaseConnection).filter(
            DatabaseConnection.conn_id == update_data["conn_id"]
        ).first()

        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Database connection with ID {update_data['conn_id']} not found"
            )

    # Verify parent snorkel job exists if updating
    if "parent_snorkel_job_id" in update_data and update_data["parent_snorkel_job_id"] is not None:
        parent_job = db.query(SnorkelJob).filter(
            SnorkelJob.job_id == update_data["parent_snorkel_job_id"],
            SnorkelJob.c_id == c_id
        ).first()
        if not parent_job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Snorkel job {update_data['parent_snorkel_job_id']} not found for concept {c_id}"
            )

    # Update fields
    for key, value in update_data.items():
        setattr(index, key, value)

    # Reset materialization status if query or key_column changed
    if "sql_query" in update_data or "query_template_params" in update_data or "key_column" in update_data:
        index.is_materialized = False
        index.materialized_at = None
        index.row_count = None
        index.storage_path = None

    db.commit()
    db.refresh(index)

    return index


@router.delete("/{c_id}/indexes/{index_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_index(
    c_id: int,
    index_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete an index.

    - **c_id**: Concept ID
    - **index_id**: Index ID
    """
    index = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.index_id == index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {index_id} not found for concept {c_id}"
        )

    db.delete(index)
    db.commit()

    return None


@router.post("/{c_id}/indexes/{index_id}/materialize", response_model=IndexMaterializeResponse)
async def materialize_index(
    c_id: int,
    index_id: int,
    db: Session = Depends(get_db)
):
    """
    Trigger Dagster to materialize an index.

    This will:
    1. Execute the SQL query
    2. Upload results to S3
    3. Update materialization status

    - **c_id**: Concept ID
    - **index_id**: Index ID
    """
    index = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.index_id == index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {index_id} not found for concept {c_id}"
        )

    # Get database connection
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.conn_id == index.conn_id
    ).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Database connection with ID {index.conn_id} not found"
        )

    # Trigger Dagster job
    from backend.utils.dagster_client import get_dagster_client
    from backend.routers.jobs import create_unified_job

    try:
        run_config = {
            "ops": {
                "materialized_index": {
                    "config": {
                        "index_id": index_id
                    }
                }
            }
        }

        dagster_client = get_dagster_client()
        result = dagster_client.submit_job_execution(
            job_name="materialize_index_job",
            run_config=run_config
        )

        # Create unified job tracker entry
        try:
            create_unified_job(
                db,
                service="snorkel",
                job_type="materialize_index",
                dagster_run_id=result["run_id"],
                dagster_job_name="materialize_index_job",
                service_job_ref={"entity_type": "concept_index", "entity_id": index_id},
                config=run_config,
                status="RUNNING",
            )
            db.commit()
        except Exception:
            pass  # Non-critical

        return IndexMaterializeResponse(
            index_id=index_id,
            dagster_run_id=result["run_id"],
            status=result["status"]
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger Dagster job: {str(e)}"
        )


# ============================================================================
# Derived Index Endpoints
# ============================================================================

def _trace_conn_id(index_id: int, db: Session, visited: set = None) -> int:
    """Walk the parent chain up to the root SQL index and return its conn_id."""
    if visited is None:
        visited = set()
    if index_id in visited:
        raise ValueError(f"Circular reference detected at index {index_id}")
    visited.add(index_id)

    idx = db.query(ConceptIndex).filter(ConceptIndex.index_id == index_id).first()
    if not idx:
        raise ValueError(f"Index {index_id} not found")
    if idx.source_type == "sql":
        return idx.conn_id
    if idx.parent_index_id:
        return _trace_conn_id(idx.parent_index_id, db, visited)
    if idx.parent_snorkel_job_id:
        job = db.query(SnorkelJob).filter(SnorkelJob.job_id == idx.parent_snorkel_job_id).first()
        if not job:
            raise ValueError(f"Snorkel job {idx.parent_snorkel_job_id} not found")
        parent_idx = db.query(ConceptIndex).filter(ConceptIndex.index_id == job.index_id).first()
        if not parent_idx:
            raise ValueError(f"Parent index {job.index_id} not found")
        return _trace_conn_id(parent_idx.index_id, db, visited)
    raise ValueError(f"Cannot trace conn_id for index {index_id}")


@router.post("/{c_id}/indexes/derived", response_model=IndexResponse, status_code=status.HTTP_201_CREATED)
async def create_derived_index(
    c_id: int,
    request: DerivedIndexCreate,
    db: Session = Depends(get_db)
):
    """
    Create a derived index (pipeline input for a CV node).

    - Root derived: set `parent_index_id` (points to a SQL index), no label_filter.
    - Child derived: set `parent_snorkel_job_id` (completed Snorkel job) + optional `label_filter`.

    The system inherits conn_id by tracing up to the root SQL index and computes
    filtered_count via entity resolution.
    """
    # Validate cv_id belongs to this concept
    cv = db.query(ConceptValue).filter(
        ConceptValue.cv_id == request.cv_id,
        ConceptValue.c_id == c_id
    ).first()
    if not cv:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept value {request.cv_id} not found in concept {c_id}"
        )

    # Must have exactly one parent reference
    if request.parent_index_id and request.parent_snorkel_job_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Set either parent_index_id or parent_snorkel_job_id, not both"
        )
    if not request.parent_index_id and not request.parent_snorkel_job_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Must set either parent_index_id or parent_snorkel_job_id"
        )

    # Validate parent exists
    if request.parent_index_id:
        parent_idx = db.query(ConceptIndex).filter(
            ConceptIndex.index_id == request.parent_index_id
        ).first()
        if not parent_idx:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Parent index {request.parent_index_id} not found"
            )

    if request.parent_snorkel_job_id:
        parent_job = db.query(SnorkelJob).filter(
            SnorkelJob.job_id == request.parent_snorkel_job_id,
            SnorkelJob.status == "COMPLETED"
        ).first()
        if not parent_job:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Completed Snorkel job {request.parent_snorkel_job_id} not found"
            )

    # Check name uniqueness
    existing = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.name == request.name
    ).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Index with name '{request.name}' already exists for concept {c_id}"
        )

    # Inherit conn_id from root SQL index
    try:
        parent_id = request.parent_index_id
        if not parent_id and request.parent_snorkel_job_id:
            parent_job = db.query(SnorkelJob).filter(
                SnorkelJob.job_id == request.parent_snorkel_job_id
            ).first()
            parent_id = parent_job.index_id
        inherited_conn_id = _trace_conn_id(parent_id, db)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot trace connection: {str(e)}"
        )

    # Compute filtered_count via entity resolution
    filtered_count = None
    try:
        from backend.utils.resolve_entities import resolve_entity_ids as _resolve
        # Create a temporary index-like object to resolve
        # We need the index to exist first, so we'll compute after creation
    except ImportError:
        pass

    index = ConceptIndex(
        c_id=c_id,
        name=request.name,
        source_type="derived",
        cv_id=request.cv_id,
        conn_id=inherited_conn_id,
        parent_index_id=request.parent_index_id,
        parent_snorkel_job_id=request.parent_snorkel_job_id,
        label_filter=request.label_filter,
        output_type=request.output_type,
        sql_query=None,
        is_materialized=True,  # Derived indexes are "materialized" by definition (virtual)
    )

    db.add(index)
    db.commit()
    db.refresh(index)

    # Compute filtered_count now that the index exists
    try:
        from backend.utils.resolve_entities import resolve_entity_ids as _resolve
        df = _resolve(index.index_id, db)
        index.filtered_count = len(df)
        index.row_count = len(df)
        db.commit()
        db.refresh(index)
    except Exception as e:
        # Non-critical — count can be computed later
        import logging
        logging.getLogger(__name__).warning(f"Failed to compute filtered_count: {e}")

    return index


@router.patch("/{c_id}/indexes/{index_id}/filter", response_model=IndexResponse)
async def update_label_filter(
    c_id: int,
    index_id: int,
    request: LabelFilterUpdate,
    db: Session = Depends(get_db)
):
    """
    Update the label filter on a derived index.

    Only valid for derived indexes with a parent_snorkel_job_id.
    Recomputes filtered_count after updating.
    """
    index = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.index_id == index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {index_id} not found for concept {c_id}"
        )

    if index.source_type != "derived":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Label filter can only be set on derived indexes"
        )

    if not index.parent_snorkel_job_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Label filter requires a parent_snorkel_job_id"
        )

    index.label_filter = request.label_filter

    # Recompute filtered_count
    try:
        from backend.utils.resolve_entities import resolve_entity_ids as _resolve
        df = _resolve(index.index_id, db)
        index.filtered_count = len(df)
        index.row_count = len(df)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to recompute filtered_count: {e}")

    db.commit()
    db.refresh(index)

    return index


@router.get("/{c_id}/indexes/{index_id}/entities", response_model=EntityPreviewResponse)
async def get_index_entities(
    c_id: int,
    index_id: int,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Preview entities from any index type (SQL or derived).

    Uses resolve_entity_ids() to get the full entity set, then paginates.
    """
    index = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.index_id == index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {index_id} not found for concept {c_id}"
        )

    try:
        from backend.utils.resolve_entities import resolve_entity_ids as _resolve
        df = _resolve(index.index_id, db)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resolve entities: {str(e)}"
        )

    total_count = len(df)
    page = df.iloc[offset:offset + limit]
    entities = page.to_dict(orient="records")

    return EntityPreviewResponse(
        index_id=index_id,
        source_type=index.source_type,
        total_count=total_count,
        entities=entities,
    )
