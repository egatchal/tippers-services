"""Read-only endpoints for browsing the space table on the tippers external DB."""
import os
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from sqlalchemy import create_engine, text

from backend.schemas import SpaceResponse

router = APIRouter()


def get_tippers_engine():
    url = os.getenv("TIPPERS_DB_URL")
    if not url:
        raise HTTPException(status_code=500, detail="TIPPERS_DB_URL is not configured")
    return create_engine(url)


def _row_to_space(row) -> SpaceResponse:
    return SpaceResponse(
        space_id=row.space_id,
        space_name=row.space_name,
        parent_space_id=row.parent_space_id,
        building_room=row.building_room,
    )


@router.get("", response_model=List[SpaceResponse])
def list_spaces(
    search: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
):
    """List spaces, optionally filtered by name."""
    engine = get_tippers_engine()
    with engine.connect() as conn:
        if search:
            result = conn.execute(
                text("""
                    SELECT space_id, space_name, parent_space_id, building_room
                    FROM space
                    WHERE space_name ILIKE :pattern
                    ORDER BY space_name
                    LIMIT :limit OFFSET :skip
                """),
                {"pattern": f"%{search}%", "limit": limit, "skip": skip},
            )
        else:
            result = conn.execute(
                text("""
                    SELECT space_id, space_name, parent_space_id, building_room
                    FROM space
                    ORDER BY space_name
                    LIMIT :limit OFFSET :skip
                """),
                {"limit": limit, "skip": skip},
            )
        return [_row_to_space(r) for r in result.fetchall()]


@router.get("/{space_id}", response_model=SpaceResponse)
def get_space(space_id: int):
    """Get a single space by ID."""
    engine = get_tippers_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT space_id, space_name, parent_space_id, building_room
                FROM space WHERE space_id = :id
            """),
            {"id": space_id},
        )
        row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Space {space_id} not found")
    return _row_to_space(row)


@router.get("/{space_id}/children", response_model=List[SpaceResponse])
def get_space_children(space_id: int):
    """Get direct children of a space."""
    engine = get_tippers_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT space_id, space_name, parent_space_id, building_room
                FROM space WHERE parent_space_id = :id
                ORDER BY space_name
            """),
            {"id": space_id},
        )
        return [_row_to_space(r) for r in result.fetchall()]


@router.get("/{space_id}/subtree", response_model=List[SpaceResponse])
def get_space_subtree(space_id: int):
    """Get all descendants of a space (including the root itself) via recursive CTE."""
    engine = get_tippers_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                WITH RECURSIVE subtree AS (
                    SELECT space_id, space_name, parent_space_id, building_room
                    FROM space WHERE space_id = :id
                    UNION ALL
                    SELECT s.space_id, s.space_name, s.parent_space_id, s.building_room
                    FROM space s JOIN subtree st ON s.parent_space_id = st.space_id
                )
                SELECT * FROM subtree ORDER BY space_name
            """),
            {"id": space_id},
        )
        return [_row_to_space(r) for r in result.fetchall()]
