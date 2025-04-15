import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL Credentials
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Database Name
DB_SOURCE = os.getenv("DB_SOURCE")
DB_LOG = os.getenv("DB_LOG")
DB_STAGING = os.getenv("DB_STAGING")
DB_WAREHOUSE = os.getenv("DB_WAREHOUSE")

# Endpoint API
MILESTONES_API = os.getenv("MILESTONES_API")

# MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_PROFILING_BUCKET_NAME = os.getenv("MINIO_PROFILING_BUCKET_NAME")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
