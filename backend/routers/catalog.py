"""Concept, ConceptValue, and Asset catalog endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import Concept, ConceptValue, ConceptIndex, ConceptRule
from backend.schemas import (
    ConceptCreate,
    ConceptUpdate,
    ConceptResponse,
    ConceptValueCreate,
    ConceptValueUpdate,
    ConceptValueResponse,
    AssetCatalogResponse,
    CatalogIndexItem,
    CatalogRuleItem
)

router = APIRouter()


# ============================================================================
# Concept CRUD Endpoints
# ============================================================================

@router.post("/", response_model=ConceptResponse, status_code=status.HTTP_201_CREATED)
async def create_concept(
    request: ConceptCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new concept.

    - **name**: Unique concept name
    - **description**: Optional description
    """
    # Check if name already exists
    existing = db.query(Concept).filter(Concept.name == request.name).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Concept with name '{request.name}' already exists"
        )

    concept = Concept(
        name=request.name,
        description=request.description
    )

    db.add(concept)
    db.commit()
    db.refresh(concept)

    return concept


@router.get("/", response_model=List[ConceptResponse])
async def list_concepts(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List all concepts.

    - **skip**: Number of records to skip (pagination)
    - **limit**: Maximum number of records to return
    """
    concepts = db.query(Concept).offset(skip).limit(limit).all()
    return concepts


@router.get("/{c_id}", response_model=ConceptResponse)
async def get_concept(
    c_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific concept by ID.

    - **c_id**: Concept ID
    """
    concept = db.query(Concept).filter(Concept.c_id == c_id).first()

    if not concept:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept with ID {c_id} not found"
        )

    return concept


@router.patch("/{c_id}", response_model=ConceptResponse)
async def update_concept(
    c_id: int,
    request: ConceptUpdate,
    db: Session = Depends(get_db)
):
    """
    Update a concept.

    - **c_id**: Concept ID
    - Only provided fields will be updated
    """
    concept = db.query(Concept).filter(Concept.c_id == c_id).first()

    if not concept:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept with ID {c_id} not found"
        )

    update_data = request.model_dump(exclude_unset=True)

    # Check name uniqueness if updating name
    if "name" in update_data:
        existing = db.query(Concept).filter(
            Concept.name == update_data["name"],
            Concept.c_id != c_id
        ).first()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Concept with name '{update_data['name']}' already exists"
            )

    for key, value in update_data.items():
        setattr(concept, key, value)

    db.commit()
    db.refresh(concept)

    return concept


@router.delete("/{c_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_concept(
    c_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a concept.

    WARNING: This will cascade delete all related concept values,
    indexes, rules, labeling functions, and Snorkel jobs.

    - **c_id**: Concept ID
    """
    concept = db.query(Concept).filter(Concept.c_id == c_id).first()

    if not concept:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept with ID {c_id} not found"
        )

    db.delete(concept)
    db.commit()

    return None


# ============================================================================
# ConceptValue CRUD Endpoints
# ============================================================================

@router.post("/{c_id}/values", response_model=ConceptValueResponse, status_code=status.HTTP_201_CREATED)
async def create_concept_value(
    c_id: int,
    request: ConceptValueCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new concept value (label).

    - **c_id**: Concept ID
    - **name**: Label name (e.g., "HIGH_VALUE", "LOW_VALUE")
    - **description**: Optional description
    - **display_order**: Optional display order
    """
    # Check if concept exists
    concept = db.query(Concept).filter(Concept.c_id == c_id).first()

    if not concept:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept with ID {c_id} not found"
        )

    # Check if name already exists for this concept
    existing = db.query(ConceptValue).filter(
        ConceptValue.c_id == c_id,
        ConceptValue.name == request.name
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Concept value with name '{request.name}' already exists for concept {c_id}"
        )

    concept_value = ConceptValue(
        c_id=c_id,
        name=request.name,
        description=request.description,
        display_order=request.display_order
    )

    db.add(concept_value)
    db.commit()
    db.refresh(concept_value)

    return concept_value


@router.get("/{c_id}/values", response_model=List[ConceptValueResponse])
async def list_concept_values(
    c_id: int,
    db: Session = Depends(get_db)
):
    """
    List all concept values (labels) for a concept.

    - **c_id**: Concept ID
    """
    values = db.query(ConceptValue).filter(
        ConceptValue.c_id == c_id
    ).order_by(ConceptValue.display_order, ConceptValue.cv_id).all()

    return values


@router.get("/{c_id}/values/{cv_id}", response_model=ConceptValueResponse)
async def get_concept_value(
    c_id: int,
    cv_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific concept value by ID.

    - **c_id**: Concept ID
    - **cv_id**: Concept value ID
    """
    value = db.query(ConceptValue).filter(
        ConceptValue.c_id == c_id,
        ConceptValue.cv_id == cv_id
    ).first()

    if not value:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept value with ID {cv_id} not found for concept {c_id}"
        )

    return value


@router.patch("/{c_id}/values/{cv_id}", response_model=ConceptValueResponse)
async def update_concept_value(
    c_id: int,
    cv_id: int,
    request: ConceptValueUpdate,
    db: Session = Depends(get_db)
):
    """
    Update a concept value.

    - **c_id**: Concept ID
    - **cv_id**: Concept value ID
    - Only provided fields will be updated
    """
    value = db.query(ConceptValue).filter(
        ConceptValue.c_id == c_id,
        ConceptValue.cv_id == cv_id
    ).first()

    if not value:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept value with ID {cv_id} not found for concept {c_id}"
        )

    update_data = request.model_dump(exclude_unset=True)

    # Check name uniqueness if updating name
    if "name" in update_data:
        existing = db.query(ConceptValue).filter(
            ConceptValue.c_id == c_id,
            ConceptValue.name == update_data["name"],
            ConceptValue.cv_id != cv_id
        ).first()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Concept value with name '{update_data['name']}' already exists for concept {c_id}"
            )

    for key, value_data in update_data.items():
        setattr(value, key, value_data)

    db.commit()
    db.refresh(value)

    return value


@router.delete("/{c_id}/values/{cv_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_concept_value(
    c_id: int,
    cv_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a concept value.

    WARNING: This will cascade delete all labeling functions
    that reference this concept value.

    - **c_id**: Concept ID
    - **cv_id**: Concept value ID
    """
    value = db.query(ConceptValue).filter(
        ConceptValue.c_id == c_id,
        ConceptValue.cv_id == cv_id
    ).first()

    if not value:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept value with ID {cv_id} not found for concept {c_id}"
        )

    db.delete(value)
    db.commit()

    return None


# ============================================================================
# Asset Catalog Endpoints
# ============================================================================


@router.get("/{c_id}/catalog", response_model=AssetCatalogResponse)
async def get_asset_catalog(
    c_id: int,
    db: Session = Depends(get_db)
):
    """
    Get all assets for a concept (indexes and rules).

    This endpoint provides a comprehensive view of all available
    data assets for building Snorkel pipelines.

    - **c_id**: Concept ID

    Returns:
    - List of indexes with materialization status
    - List of rules with materialization status
    """
    # Get all indexes for this concept
    indexes = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id
    ).all()

    # Get all rules for this concept
    rules = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id
    ).all()

    # Format indexes
    index_items = [
        CatalogIndexItem(
            id=idx.index_id,
            name=idx.name,
            is_materialized=idx.is_materialized,
            row_count=idx.row_count,
            storage_path=idx.storage_path
        )
        for idx in indexes
    ]

    # Format rules
    rule_items = [
        CatalogRuleItem(
            id=rule.r_id,
            name=rule.name,
            is_materialized=rule.is_materialized,
            row_count=rule.row_count,
            storage_path=rule.storage_path
        )
        for rule in rules
    ]

    return AssetCatalogResponse(
        indexes=index_items,
        rules=rule_items
    )


@router.get("/{c_id}/catalog/materialized", response_model=AssetCatalogResponse)
async def get_materialized_assets(
    c_id: int,
    db: Session = Depends(get_db)
):
    """
    Get only materialized assets for a concept.

    This is useful for showing only ready-to-use assets
    in the pipeline builder.

    - **c_id**: Concept ID
    """
    # Get materialized indexes
    indexes = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.is_materialized == True
    ).all()

    # Get materialized rules
    rules = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id,
        ConceptRule.is_materialized == True
    ).all()

    # Format indexes
    index_items = [
        CatalogIndexItem(
            id=idx.index_id,
            name=idx.name,
            is_materialized=idx.is_materialized,
            row_count=idx.row_count,
            storage_path=idx.storage_path
        )
        for idx in indexes
    ]

    # Format rules
    rule_items = [
        CatalogRuleItem(
            id=rule.r_id,
            name=rule.name,
            is_materialized=rule.is_materialized,
            row_count=rule.row_count,
            storage_path=rule.storage_path
        )
        for rule in rules
    ]

    return AssetCatalogResponse(
        indexes=index_items,
        rules=rule_items
    )


@router.get("/{c_id}/catalog/stats")
async def get_catalog_stats(
    c_id: int,
    db: Session = Depends(get_db)
):
    """
    Get catalog statistics for a concept.

    - **c_id**: Concept ID

    Returns:
    - Total number of indexes
    - Number of materialized indexes
    - Total number of rules
    - Number of materialized rules
    - Total data rows across all materialized assets
    """
    # Index stats
    total_indexes = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id
    ).count()

    materialized_indexes = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.is_materialized == True
    ).count()

    # Rule stats
    total_rules = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id
    ).count()

    materialized_rules = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id,
        ConceptRule.is_materialized == True
    ).count()

    # Calculate total rows
    from sqlalchemy import func

    total_index_rows = db.query(func.sum(ConceptIndex.row_count)).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.is_materialized == True
    ).scalar() or 0

    total_rule_rows = db.query(func.sum(ConceptRule.row_count)).filter(
        ConceptRule.c_id == c_id,
        ConceptRule.is_materialized == True
    ).scalar() or 0

    return {
        "concept_id": c_id,
        "indexes": {
            "total": total_indexes,
            "materialized": materialized_indexes,
            "unmaterialized": total_indexes - materialized_indexes,
            "total_rows": total_index_rows
        },
        "rules": {
            "total": total_rules,
            "materialized": materialized_rules,
            "unmaterialized": total_rules - materialized_rules,
            "total_rows": total_rule_rows
        },
        "summary": {
            "total_assets": total_indexes + total_rules,
            "materialized_assets": materialized_indexes + materialized_rules,
            "total_data_rows": total_index_rows + total_rule_rows
        }
    }
