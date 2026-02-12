"""Labeling function CRUD endpoints with versioning support."""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import LabelingFunction, ConceptRule, ConceptValue
from backend.schemas import (
    LabelingFunctionCreate,
    LabelingFunctionUpdate,
    LabelingFunctionResponse,
    LabelingFunctionVersionCreate
)
from datetime import datetime

router = APIRouter()


def validate_custom_python(code: str, allowed_imports: List[str]):
    """
    Validate custom Python code for security.

    This is a basic validation. In production, use a more robust
    sandboxing solution like RestrictedPython or run in isolated containers.
    """
    # Disallowed keywords for security
    disallowed = [
        "import os", "import sys", "import subprocess", "__import__",
        "eval(", "exec(", "compile(", "open(", "file(", "input(",
        "globals(", "locals(", "vars(", "dir("
    ]

    for keyword in disallowed:
        if keyword in code.lower():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Disallowed keyword '{keyword}' found in custom code"
            )

    # Validate allowed imports
    for module in allowed_imports:
        if module not in ["re", "datetime", "math", "statistics"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Module '{module}' is not in allowed imports list"
            )


def generate_lf_template(concept_values: list) -> str:
    """
    Auto-generate a labeling function template from concept values.

    Args:
        concept_values: list of (cv_id, name) tuples
    """
    lines = []

    # Generate constants for each concept value
    for cv_id, name in concept_values:
        lines.append(f"{name.upper()} = {cv_id}")
    lines.append("ABSTAIN = -1")
    lines.append("")
    lines.append("def labeling_function(row):")
    lines.append("    # TODO: implement voting logic")
    lines.append("    # Available columns: row['column_name']")

    # Add example condition for the first concept value
    if concept_values:
        first_cv_id, first_name = concept_values[0]
        lines.append(f"    # Example: return {first_name.upper()} if condition met")

    lines.append("    return ABSTAIN")

    return "\n".join(lines)


@router.post("/{c_id}/labeling-functions", response_model=LabelingFunctionResponse, status_code=status.HTTP_201_CREATED)
async def create_labeling_function(
    c_id: int,
    request: LabelingFunctionCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new custom labeling function.

    - **c_id**: Concept ID
    - **name**: LF name
    - **rule_id**: Rule ID that provides features (REQUIRED)
    - **applicable_cv_ids**: Concept value IDs this LF can vote on
    - **code**: Python code (if omitted, a template is auto-generated)
    - **allowed_imports**: List of allowed import modules
    - **parent_lf_id**: Optional parent LF ID for versioning
    """
    # Verify rule exists and belongs to concept
    rule = db.query(ConceptRule).filter(
        ConceptRule.c_id == c_id,
        ConceptRule.r_id == request.rule_id
    ).first()

    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule with ID {request.rule_id} not found for concept {c_id}"
        )

    # Validate applicable_cv_ids — batch lookup
    concept_values = db.query(ConceptValue).filter(
        ConceptValue.c_id == c_id,
        ConceptValue.cv_id.in_(request.applicable_cv_ids)
    ).all()

    found_cv_ids = {cv.cv_id for cv in concept_values}
    missing_cv_ids = set(request.applicable_cv_ids) - found_cv_ids

    if missing_cv_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept value IDs {sorted(missing_cv_ids)} not found for concept {c_id}"
        )

    # Check if name already exists
    existing = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.name == request.name
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Labeling function with name '{request.name}' already exists for concept {c_id}"
        )

    # Determine version number
    version = 1
    if request.parent_lf_id:
        parent = db.query(LabelingFunction).filter(
            LabelingFunction.lf_id == request.parent_lf_id
        ).first()

        if not parent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Parent LF with ID {request.parent_lf_id} not found"
            )

        version = parent.version + 1

    # Build code — use provided or auto-generate template
    code = request.code
    if not code:
        cv_tuples = [(cv.cv_id, cv.name) for cv in concept_values]
        code = generate_lf_template(cv_tuples)
    else:
        validate_custom_python(code, request.allowed_imports)

    # Create LF
    lf = LabelingFunction(
        c_id=c_id,
        applicable_cv_ids=request.applicable_cv_ids,
        rule_id=request.rule_id,
        name=request.name,
        version=version,
        parent_lf_id=request.parent_lf_id,
        lf_type="custom",
        lf_config={
            "code": code,
            "allowed_imports": request.allowed_imports
        },
        is_active=False,
        requires_approval=True
    )

    db.add(lf)
    db.commit()
    db.refresh(lf)

    return lf


@router.get("/{c_id}/labeling-functions/template")
async def preview_lf_template(
    c_id: int,
    applicable_cv_ids: List[int] = Query(..., description="Concept value IDs to include in template"),
    db: Session = Depends(get_db)
):
    """
    Preview an auto-generated labeling function template without creating an LF.

    - **c_id**: Concept ID
    - **applicable_cv_ids**: Concept value IDs to include in template
    """
    concept_values = db.query(ConceptValue).filter(
        ConceptValue.c_id == c_id,
        ConceptValue.cv_id.in_(applicable_cv_ids)
    ).all()

    found_cv_ids = {cv.cv_id for cv in concept_values}
    missing_cv_ids = set(applicable_cv_ids) - found_cv_ids

    if missing_cv_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept value IDs {sorted(missing_cv_ids)} not found for concept {c_id}"
        )

    cv_tuples = [(cv.cv_id, cv.name) for cv in concept_values]
    code = generate_lf_template(cv_tuples)

    return {"code": code, "applicable_cv_ids": applicable_cv_ids}


@router.post("/{c_id}/labeling-functions/{lf_id}/versions", response_model=LabelingFunctionResponse, status_code=status.HTTP_201_CREATED)
async def create_lf_version(
    c_id: int,
    lf_id: int,
    request: LabelingFunctionVersionCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new version of an existing labeling function.

    This creates a new LF with:
    - Incremented version number
    - parent_lf_id pointing to the original
    - Same rule_id and applicable_cv_ids
    - New lf_config

    - **c_id**: Concept ID
    - **lf_id**: Parent labeling function ID
    - **lf_config**: New configuration for this version
    - **name**: Optional new name for this version
    """
    # Get parent LF
    parent_lf = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.lf_id == lf_id
    ).first()

    if not parent_lf:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Labeling function with ID {lf_id} not found for concept {c_id}"
        )

    # Deprecate parent if it's active
    if parent_lf.is_active:
        parent_lf.is_active = False
        parent_lf.deprecated_at = datetime.utcnow()

    # Create new version
    new_name = request.name if request.name else f"{parent_lf.name}_v{parent_lf.version + 1}"

    new_lf = LabelingFunction(
        c_id=c_id,
        applicable_cv_ids=parent_lf.applicable_cv_ids,
        rule_id=parent_lf.rule_id,
        name=new_name,
        version=parent_lf.version + 1,
        parent_lf_id=lf_id,
        lf_type="custom",
        lf_config=request.lf_config,
        is_active=False,
        requires_approval=True
    )

    db.add(new_lf)

    # Update parent to reference new version
    parent_lf.deprecated_by_lf_id = new_lf.lf_id

    db.commit()
    db.refresh(new_lf)
    db.refresh(parent_lf)

    return new_lf


@router.get("/{c_id}/labeling-functions/{lf_id}/versions", response_model=List[LabelingFunctionResponse])
async def list_lf_versions(
    c_id: int,
    lf_id: int,
    db: Session = Depends(get_db)
):
    """
    List all versions of a labeling function.

    Returns all versions in the version chain, ordered by version number.

    - **c_id**: Concept ID
    - **lf_id**: Any labeling function ID in the version chain
    """
    # Get the specified LF
    lf = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.lf_id == lf_id
    ).first()

    if not lf:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Labeling function with ID {lf_id} not found for concept {c_id}"
        )

    # Find the root (earliest version)
    root_lf = lf
    while root_lf.parent_lf_id:
        root_lf = db.query(LabelingFunction).filter(
            LabelingFunction.lf_id == root_lf.parent_lf_id
        ).first()

    # Collect all versions starting from root
    versions = [root_lf]
    current = root_lf

    while current.deprecated_by_lf_id:
        next_version = db.query(LabelingFunction).filter(
            LabelingFunction.lf_id == current.deprecated_by_lf_id
        ).first()
        if next_version:
            versions.append(next_version)
            current = next_version
        else:
            break

    return versions


@router.get("/{c_id}/labeling-functions", response_model=List[LabelingFunctionResponse])
async def list_labeling_functions(
    c_id: int,
    skip: int = 0,
    limit: int = 100,
    active_only: bool = False,
    db: Session = Depends(get_db)
):
    """
    List all labeling functions for a concept.

    - **c_id**: Concept ID
    - **skip**: Number of records to skip (pagination)
    - **limit**: Maximum number of records to return
    - **active_only**: Only return active labeling functions
    """
    query = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id
    )

    if active_only:
        query = query.filter(LabelingFunction.is_active == True)

    lfs = query.offset(skip).limit(limit).all()
    return lfs


@router.get("/{c_id}/labeling-functions/{lf_id}", response_model=LabelingFunctionResponse)
async def get_labeling_function(
    c_id: int,
    lf_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific labeling function by ID.

    - **c_id**: Concept ID
    - **lf_id**: Labeling function ID
    """
    lf = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.lf_id == lf_id
    ).first()

    if not lf:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Labeling function with ID {lf_id} not found for concept {c_id}"
        )

    return lf


@router.get("/{c_id}/labeling-functions/{lf_id}/metrics")
async def get_lf_metrics(
    c_id: int,
    lf_id: int,
    db: Session = Depends(get_db)
):
    """
    Get performance metrics for a labeling function.

    Returns estimated accuracy, coverage, and conflicts
    populated after Snorkel training.

    - **c_id**: Concept ID
    - **lf_id**: Labeling function ID
    """
    lf = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.lf_id == lf_id
    ).first()

    if not lf:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Labeling function with ID {lf_id} not found for concept {c_id}"
        )

    return {
        "lf_id": lf_id,
        "name": lf.name,
        "version": lf.version,
        "estimated_accuracy": lf.estimated_accuracy,
        "coverage": lf.coverage,
        "conflicts": lf.conflicts,
        "metrics_available": lf.estimated_accuracy is not None
    }


@router.patch("/{c_id}/labeling-functions/{lf_id}", response_model=LabelingFunctionResponse)
async def update_labeling_function(
    c_id: int,
    lf_id: int,
    request: LabelingFunctionUpdate,
    db: Session = Depends(get_db)
):
    """
    Update a labeling function.

    - **c_id**: Concept ID
    - **lf_id**: Labeling function ID
    - Only provided fields will be updated
    """
    lf = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.lf_id == lf_id
    ).first()

    if not lf:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Labeling function with ID {lf_id} not found for concept {c_id}"
        )

    # Get update data
    update_data = request.model_dump(exclude_unset=True)

    # Check name uniqueness if updating name
    if "name" in update_data:
        existing = db.query(LabelingFunction).filter(
            LabelingFunction.c_id == c_id,
            LabelingFunction.name == update_data["name"],
            LabelingFunction.lf_id != lf_id
        ).first()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Labeling function with name '{update_data['name']}' already exists for concept {c_id}"
            )

    # Validate applicable_cv_ids if updating
    if "applicable_cv_ids" in update_data:
        cv_ids = update_data["applicable_cv_ids"]
        if cv_ids:
            found = db.query(ConceptValue.cv_id).filter(
                ConceptValue.c_id == c_id,
                ConceptValue.cv_id.in_(cv_ids)
            ).all()
            found_ids = {row[0] for row in found}
            missing = set(cv_ids) - found_ids
            if missing:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Concept value IDs {sorted(missing)} not found for concept {c_id}"
                )

    # Update fields
    for key, value in update_data.items():
        setattr(lf, key, value)

    db.commit()
    db.refresh(lf)

    return lf


@router.delete("/{c_id}/labeling-functions/{lf_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_labeling_function(
    c_id: int,
    lf_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a labeling function.

    - **c_id**: Concept ID
    - **lf_id**: Labeling function ID
    """
    lf = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.lf_id == lf_id
    ).first()

    if not lf:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Labeling function with ID {lf_id} not found for concept {c_id}"
        )

    db.delete(lf)
    db.commit()

    return None


@router.post("/{c_id}/labeling-functions/{lf_id}/approve", response_model=LabelingFunctionResponse)
async def approve_labeling_function(
    c_id: int,
    lf_id: int,
    db: Session = Depends(get_db)
):
    """
    Approve a custom labeling function (activates it).

    - **c_id**: Concept ID
    - **lf_id**: Labeling function ID
    """
    lf = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.lf_id == lf_id
    ).first()

    if not lf:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Labeling function with ID {lf_id} not found for concept {c_id}"
        )

    if not lf.requires_approval:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This labeling function does not require approval"
        )

    lf.is_active = True
    lf.requires_approval = False

    db.commit()
    db.refresh(lf)

    return lf


@router.post("/{c_id}/labeling-functions/{lf_id}/toggle", response_model=LabelingFunctionResponse)
async def toggle_labeling_function(
    c_id: int,
    lf_id: int,
    db: Session = Depends(get_db)
):
    """
    Toggle a labeling function's active status.

    - **c_id**: Concept ID
    - **lf_id**: Labeling function ID
    """
    lf = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.lf_id == lf_id
    ).first()

    if not lf:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Labeling function with ID {lf_id} not found for concept {c_id}"
        )

    lf.is_active = not lf.is_active

    db.commit()
    db.refresh(lf)

    return lf
