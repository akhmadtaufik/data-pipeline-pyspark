import psycopg2
from src.utils.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


def read_db(db_name: str):
    """
    Establishes a connection to a PostgreSQL database using the provided database name.

    Parameters:
        db_name (str): The name of the database to connect to.

    Returns:
        connection: A connection object to the specified PostgreSQL database.
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=db_name,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    return conn
