"""Database connection CRUD endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import DatabaseConnection
from backend.schemas import (
    DatabaseConnectionCreate,
    DatabaseConnectionUpdate,
    DatabaseConnectionResponse
)
from cryptography.fernet import Fernet
import os

router = APIRouter()

# Encryption key (should be stored securely in environment variable)
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", Fernet.generate_key())
cipher_suite = Fernet(ENCRYPTION_KEY)


def encrypt_password(password: str) -> str:
    """Encrypt password."""
    return cipher_suite.encrypt(password.encode()).decode()


def decrypt_password(encrypted_password: str) -> str:
    """Decrypt password."""
    return cipher_suite.decrypt(encrypted_password.encode()).decode()


@router.post("/database-connections", response_model=DatabaseConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_database_connection(
    request: DatabaseConnectionCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new database connection.

    - **name**: Unique connection name
    - **connection_type**: Database type (postgresql, mysql, etc.)
    - **host**: Database host
    - **port**: Database port
    - **database**: Database name
    - **user**: Database user
    - **password**: Database password (will be encrypted)
    """
    # Check if name already exists
    existing = db.query(DatabaseConnection).filter(
        DatabaseConnection.name == request.name
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Database connection with name '{request.name}' already exists"
        )

    # Encrypt password
    encrypted_password = encrypt_password(request.password)

    # Create connection
    db_connection = DatabaseConnection(
        name=request.name,
        connection_type=request.connection_type,
        host=request.host,
        port=request.port,
        database=request.database,
        user=request.user,
        encrypted_password=encrypted_password
    )

    db.add(db_connection)
    db.commit()
    db.refresh(db_connection)

    return db_connection


@router.get("/database-connections", response_model=List[DatabaseConnectionResponse])
async def list_database_connections(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List all database connections.

    - **skip**: Number of records to skip (pagination)
    - **limit**: Maximum number of records to return
    """
    connections = db.query(DatabaseConnection).offset(skip).limit(limit).all()
    return connections


@router.get("/database-connections/{conn_id}", response_model=DatabaseConnectionResponse)
async def get_database_connection(
    conn_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific database connection by ID.

    - **conn_id**: Database connection ID
    """
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.conn_id == conn_id
    ).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Database connection with ID {conn_id} not found"
        )

    return connection


@router.patch("/database-connections/{conn_id}", response_model=DatabaseConnectionResponse)
async def update_database_connection(
    conn_id: int,
    request: DatabaseConnectionUpdate,
    db: Session = Depends(get_db)
):
    """
    Update a database connection.

    - **conn_id**: Database connection ID
    - Only provided fields will be updated
    """
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.conn_id == conn_id
    ).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Database connection with ID {conn_id} not found"
        )

    # Update fields
    update_data = request.model_dump(exclude_unset=True)

    # Check for name uniqueness if updating name
    if "name" in update_data:
        existing = db.query(DatabaseConnection).filter(
            DatabaseConnection.name == update_data["name"],
            DatabaseConnection.conn_id != conn_id
        ).first()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Database connection with name '{update_data['name']}' already exists"
            )

    # Encrypt password if provided
    if "password" in update_data:
        update_data["encrypted_password"] = encrypt_password(update_data.pop("password"))

    for key, value in update_data.items():
        setattr(connection, key, value)

    db.commit()
    db.refresh(connection)

    return connection


@router.delete("/database-connections/{conn_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_database_connection(
    conn_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a database connection.

    - **conn_id**: Database connection ID
    """
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.conn_id == conn_id
    ).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Database connection with ID {conn_id} not found"
        )

    db.delete(connection)
    db.commit()

    return None


@router.post("/database-connections/{conn_id}/test")
async def test_database_connection(
    conn_id: int,
    db: Session = Depends(get_db)
):
    """
    Test a database connection.

    - **conn_id**: Database connection ID
    """
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.conn_id == conn_id
    ).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Database connection with ID {conn_id} not found"
        )

    try:
        # Decrypt password
        password = decrypt_password(connection.encrypted_password)

        # Create connection string
        from sqlalchemy import create_engine
        if connection.connection_type == "postgresql":
            conn_string = f"postgresql://{connection.user}:{password}@{connection.host}:{connection.port}/{connection.database}"
        elif connection.connection_type == "mysql":
            conn_string = f"mysql://{connection.user}:{password}@{connection.host}:{connection.port}/{connection.database}"
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported connection type: {connection.connection_type}"
            )

        # Test connection
        from sqlalchemy import text
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return {"status": "success", "message": "Connection successful"}

    except Exception as e:
        import traceback
        error_detail = f"{type(e).__name__}: {str(e)}"
        print(f"Connection test failed: {error_detail}")
        print(traceback.format_exc())
        return {"status": "failed", "message": error_detail}
