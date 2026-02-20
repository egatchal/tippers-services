from dagster import ConfigurableResource
import boto3
import psycopg2
from typing import Optional
import os

class DatabaseResource(ConfigurableResource):
    """Database connection resource."""
    host: str
    port: int
    database: str
    user: str
    password: str

    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

class S3Resource(ConfigurableResource):
    """S3 storage resource."""
    bucket_name: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: str = "us-east-1"

    def get_client(self):
        """Get S3 client."""
        return boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        )

class MLflowResource(ConfigurableResource):
    """MLflow tracking resource."""
    tracking_uri: str
    experiment_name: str = "snorkel_training"

    def get_client(self):
        """Get MLflow client."""
        import mlflow
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(self.experiment_name)
        return mlflow

# Resource instances
database_resource = DatabaseResource(
    host="localhost",
    port=5432,
    database="tippers",
    user="postgres",
    password="postgres"
)

s3_resource = S3Resource(
    bucket_name=os.getenv("S3_BUCKET_NAME", "tippers-data"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
)

mlflow_resource = MLflowResource(
    tracking_uri="http://localhost:5000"
)
