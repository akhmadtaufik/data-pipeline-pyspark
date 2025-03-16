import os
from dotenv import load_dotenv

load_dotenv()

# Kredensial PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DB_SOURCE = os.getenv("DB_SOURCE")
DB_LOG = os.getenv("DB_LOG")
DB_STAGING = os.getenv("DB_STAGING")

# Endpoint API
MILESTONES_API = os.getenv("MILESTONES_API")
