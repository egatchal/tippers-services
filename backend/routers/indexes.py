"""Index CRUD and materialization endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import ConceptIndex, DatabaseConnection
from backend.schemas import (
    IndexCreate,
    IndexUpdate,
    IndexResponse,
    IndexMaterializeResponse
)

router = APIRouter()


@router.post("/{c_id}/indexes", response_model=IndexResponse, status_code=status.HTTP_201_CREATED)
async def create_index(
    c_id: int,
    request: IndexCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new index definition.

    - **c_id**: Concept ID
    - **name**: Index name (must be unique within concept)
    - **conn_id**: Database connection ID
    - **sql_query**: SQL query to execute (supports Jinja2 templates)
    - **query_template_params**: Optional template parameters
    - **partition_type**: Optional partitioning (time, id_range, categorical)
    - **partition_config**: Partition configuration
    """
    # Validate SQL syntax (basic validation)
    if not request.sql_query.strip().upper().startswith("SELECT"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="SQL query must be a SELECT statement"
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

    # Create index
    index = ConceptIndex(
        c_id=c_id,
        name=request.name,
        conn_id=request.conn_id,
        sql_query=request.sql_query,
        query_template_params=request.query_template_params,
        partition_type=request.partition_type,
        partition_config=request.partition_config,
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
        if not update_data["sql_query"].strip().upper().startswith("SELECT"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="SQL query must be a SELECT statement"
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

    # Update fields
    for key, value in update_data.items():
        setattr(index, key, value)

    # Reset materialization status if query changed
    if "sql_query" in update_data or "query_template_params" in update_data:
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
