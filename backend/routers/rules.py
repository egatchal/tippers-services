"""Rule CRUD and materialization endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import ConceptRule, ConceptIndex
from backend.schemas import (
    RuleCreate,
    RuleUpdate,
    RuleResponse,
    RuleMaterializeResponse
)

router = APIRouter()


@router.post("/{c_id}/rules", response_model=RuleResponse, status_code=status.HTTP_201_CREATED)
async def create_rule(
    c_id: int,
    request: RuleCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new rule/feature definition.

    - **c_id**: Concept ID
    - **name**: Rule name (must be unique within concept)
    - **index_id**: Index ID to use as sample set
    - **sql_query**: SQL query with :index_values placeholder (e.g., WHERE user_id IN (:index_values))
    - **index_column**: Column from index to extract for filtering (e.g., 'user_id')
    - **query_template_params**: Optional additional template parameters
    - **partition_type**: Optional partitioning (time, id_range, categorical)
    - **partition_config**: Partition configuration
    """
    # Validate SQL syntax (basic validation)
    query_upper = request.sql_query.strip().upper()
    if not (query_upper.startswith("SELECT") or query_upper.startswith("WITH")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="SQL query must be a SELECT statement or CTE (WITH clause)"
        )

    # Check if name already exists for this concept
    existing = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id,
        ConceptRule.name == request.name
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Rule with name '{request.name}' already exists for concept {c_id}"
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

    # Verify index is materialized
    if not index.is_materialized:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Index {request.index_id} must be materialized before creating rules"
        )

    # Create rule
    rule = ConceptRule(
        c_id=c_id,
        name=request.name,
        index_id=request.index_id,
        sql_query=request.sql_query,
        index_column=request.index_column,
        query_template_params=request.query_template_params,
        applicable_cv_ids=request.applicable_cv_ids,
        label_guidance=request.label_guidance,
        partition_type=request.partition_type,
        partition_config=request.partition_config,
        is_materialized=False
    )

    db.add(rule)
    db.commit()
    db.refresh(rule)

    return rule


@router.get("/{c_id}/rules", response_model=List[RuleResponse])
async def list_rules(
    c_id: int,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List all rules for a concept.

    - **c_id**: Concept ID
    - **skip**: Number of records to skip (pagination)
    - **limit**: Maximum number of records to return
    """
    rules = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id
    ).offset(skip).limit(limit).all()

    return rules


@router.get("/{c_id}/rules/{r_id}", response_model=RuleResponse)
async def get_rule(
    c_id: int,
    r_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific rule by ID.

    - **c_id**: Concept ID
    - **r_id**: Rule ID
    """
    rule = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id,
        ConceptRule.r_id == r_id
    ).first()

    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule with ID {r_id} not found for concept {c_id}"
        )

    return rule


@router.patch("/{c_id}/rules/{r_id}", response_model=RuleResponse)
async def update_rule(
    c_id: int,
    r_id: int,
    request: RuleUpdate,
    db: Session = Depends(get_db)
):
    """
    Update a rule definition.

    - **c_id**: Concept ID
    - **r_id**: Rule ID
    - Only provided fields will be updated
    """
    rule = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id,
        ConceptRule.r_id == r_id
    ).first()

    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule with ID {r_id} not found for concept {c_id}"
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
        existing = db.query(ConceptRule).filter(
            ConceptRule.c_id == c_id,
            ConceptRule.name == update_data["name"],
            ConceptRule.r_id != r_id
        ).first()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Rule with name '{update_data['name']}' already exists for concept {c_id}"
            )

    # Verify index exists if updating index_id
    if "index_id" in update_data:
        index = db.query(ConceptIndex).filter(
            ConceptIndex.index_id == update_data["index_id"]
        ).first()

        if not index:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Index with ID {update_data['index_id']} not found"
            )

        if index.c_id != c_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Index {update_data['index_id']} does not belong to concept {c_id}"
            )

    # Update fields
    for key, value in update_data.items():
        setattr(rule, key, value)

    # Reset materialization status if query changed
    if "sql_query" in update_data or "query_template_params" in update_data:
        rule.is_materialized = False
        rule.materialized_at = None
        rule.row_count = None
        rule.storage_path = None

    db.commit()
    db.refresh(rule)

    return rule


@router.delete("/{c_id}/rules/{r_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_rule(
    c_id: int,
    r_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a rule.

    - **c_id**: Concept ID
    - **r_id**: Rule ID
    """
    rule = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id,
        ConceptRule.r_id == r_id
    ).first()

    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule with ID {r_id} not found for concept {c_id}"
        )

    db.delete(rule)
    db.commit()

    return None


@router.post("/{c_id}/rules/{r_id}/materialize", response_model=RuleMaterializeResponse)
async def materialize_rule(
    c_id: int,
    r_id: int,
    db: Session = Depends(get_db)
):
    """
    Trigger Dagster to materialize a rule.

    This will:
    1. Execute the SQL query
    2. Upload results to S3
    3. Update materialization status

    - **c_id**: Concept ID
    - **r_id**: Rule ID
    """
    rule = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id,
        ConceptRule.r_id == r_id
    ).first()

    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule with ID {r_id} not found for concept {c_id}"
        )

    # Verify index is materialized
    index = db.query(ConceptIndex).filter(
        ConceptIndex.index_id == rule.index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {rule.index_id} not found"
        )

    if not index.is_materialized:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Index {rule.index_id} must be materialized before materializing rules"
        )

    # Trigger Dagster job
    from backend.utils.dagster_client import get_dagster_client

    try:
        run_config = {
            "ops": {
                "materialized_rule": {
                    "config": {
                        "rule_id": r_id
                    }
                }
            }
        }

        dagster_client = get_dagster_client()
        result = dagster_client.submit_job_execution(
            job_name="materialize_rule_job",
            run_config=run_config
        )

        return RuleMaterializeResponse(
            r_id=r_id,
            dagster_run_id=result["run_id"],
            status=result["status"]
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger Dagster job: {str(e)}"
        )
