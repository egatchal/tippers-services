"""Feature CRUD and materialization endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import ConceptFeature, ConceptIndex
from backend.schemas import (
    FeatureCreate,
    FeatureUpdate,
    FeatureResponse,
    FeatureMaterializeResponse
)

router = APIRouter()


@router.post("/{c_id}/features", response_model=FeatureResponse, status_code=status.HTTP_201_CREATED)
async def create_feature(
    c_id: int,
    request: FeatureCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new feature definition.

    - **c_id**: Concept ID
    - **name**: Feature name (must be unique within concept)
    - **index_id**: Index ID to use as sample set
    - **sql_query**: SQL query with :index_values placeholder
    """
    # Validate SQL syntax
    query_upper = request.sql_query.strip().upper()
    if not (query_upper.startswith("SELECT") or query_upper.startswith("WITH")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="SQL query must be a SELECT statement or CTE (WITH clause)"
        )

    # Check if name already exists for this concept
    existing = db.query(ConceptFeature).filter(
        ConceptFeature.c_id == c_id,
        ConceptFeature.name == request.name
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Feature with name '{request.name}' already exists for concept {c_id}"
        )

    # Verify index exists and belongs to this concept
    index = db.query(ConceptIndex).filter(
        ConceptIndex.index_id == request.index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {request.index_id} not found"
        )

    if index.c_id != c_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Index {request.index_id} does not belong to concept {c_id}"
        )

    # Create feature
    feature = ConceptFeature(
        c_id=c_id,
        name=request.name,
        index_id=request.index_id,
        sql_query=request.sql_query,
        index_column=request.index_column,
        columns=request.columns,
        query_template_params=request.query_template_params,
        description=request.description,
        level=request.level,
        partition_type=request.partition_type,
        partition_config=request.partition_config,
        is_materialized=False
    )

    db.add(feature)
    db.commit()
    db.refresh(feature)

    return feature


@router.get("/{c_id}/features", response_model=List[FeatureResponse])
async def list_features(
    c_id: int,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List all features for a concept.

    - **c_id**: Concept ID
    - **skip**: Number of records to skip (pagination)
    - **limit**: Maximum number of records to return
    """
    features = db.query(ConceptFeature).filter(
        ConceptFeature.c_id == c_id
    ).offset(skip).limit(limit).all()

    return features


@router.get("/{c_id}/features/{feature_id}", response_model=FeatureResponse)
async def get_feature(
    c_id: int,
    feature_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific feature by ID.

    - **c_id**: Concept ID
    - **feature_id**: Feature ID
    """
    feature = db.query(ConceptFeature).filter(
        ConceptFeature.c_id == c_id,
        ConceptFeature.feature_id == feature_id
    ).first()

    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found for concept {c_id}"
        )

    return feature


@router.patch("/{c_id}/features/{feature_id}", response_model=FeatureResponse)
async def update_feature(
    c_id: int,
    feature_id: int,
    request: FeatureUpdate,
    db: Session = Depends(get_db)
):
    """
    Update a feature definition.

    - **c_id**: Concept ID
    - **feature_id**: Feature ID
    - Only provided fields will be updated
    """
    feature = db.query(ConceptFeature).filter(
        ConceptFeature.c_id == c_id,
        ConceptFeature.feature_id == feature_id
    ).first()

    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found for concept {c_id}"
        )

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
        existing = db.query(ConceptFeature).filter(
            ConceptFeature.c_id == c_id,
            ConceptFeature.name == update_data["name"],
            ConceptFeature.feature_id != feature_id
        ).first()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Feature with name '{update_data['name']}' already exists for concept {c_id}"
            )

    # Update fields
    for key, value in update_data.items():
        setattr(feature, key, value)

    # Reset materialization status if query changed
    if "sql_query" in update_data or "query_template_params" in update_data:
        feature.is_materialized = False
        feature.materialized_at = None
        feature.row_count = None
        feature.storage_path = None

    db.commit()
    db.refresh(feature)

    return feature


@router.delete("/{c_id}/features/{feature_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_feature(
    c_id: int,
    feature_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a feature.

    - **c_id**: Concept ID
    - **feature_id**: Feature ID
    """
    feature = db.query(ConceptFeature).filter(
        ConceptFeature.c_id == c_id,
        ConceptFeature.feature_id == feature_id
    ).first()

    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found for concept {c_id}"
        )

    db.delete(feature)
    db.commit()

    return None


@router.post("/{c_id}/features/{feature_id}/materialize", response_model=FeatureMaterializeResponse)
async def materialize_feature(
    c_id: int,
    feature_id: int,
    db: Session = Depends(get_db)
):
    """
    Trigger Dagster to materialize a feature.

    This will:
    1. Execute the SQL query against the external database
    2. Upload results to S3
    3. Update materialization status

    - **c_id**: Concept ID
    - **feature_id**: Feature ID
    """
    feature = db.query(ConceptFeature).filter(
        ConceptFeature.c_id == c_id,
        ConceptFeature.feature_id == feature_id
    ).first()

    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found for concept {c_id}"
        )

    # Verify index is materialized
    index = db.query(ConceptIndex).filter(
        ConceptIndex.index_id == feature.index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {feature.index_id} not found"
        )

    if not index.is_materialized:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Index {feature.index_id} must be materialized before materializing features"
        )

    # Trigger Dagster job
    from backend.utils.dagster_client import get_dagster_client

    try:
        run_config = {
            "ops": {
                "materialized_feature": {
                    "config": {
                        "feature_id": feature_id
                    }
                }
            }
        }

        dagster_client = get_dagster_client()
        result = dagster_client.submit_job_execution(
            job_name="materialize_feature_job",
            run_config=run_config
        )

        return FeatureMaterializeResponse(
            feature_id=feature_id,
            dagster_run_id=result["run_id"],
            status=result["status"]
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger Dagster job: {str(e)}"
        )
