import os
from typing import Any, List
from pyspark.sql import DataFrame
from src.utils.spark_session import init_spark_session


def extract_table_name(db_name: str) -> List[Any] | None:
    spark = init_spark_session()

    # set variable for database
    HOST = os.getenv("POSTGRES_HOST")
    PORT = os.getenv("POSTGRES_PORT")
    USER = os.getenv("POSTGRES_USER")
    PASSWORD = os.getenv("POSTGRES_PASSWORD")

    DB_URL = f"jdbc:postgresql://{HOST}:{PORT}/{db_name}"
    # set config
    jdbc_url = DB_URL
    connection_properties = {
        "user": USER,
        "password": PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    try:
        tables_query = "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') as all_tables"
        tables_df = spark.read.jdbc(
            url=jdbc_url, table=tables_query, properties=connection_properties  # type: ignore
        )

        # Kumpulkan nama tabel dari hasil query
        table_names = [row.table_name for row in tables_df.collect()]

        return table_names

    except Exception as e:
        error_message = str(e)
        print(f"Error connecting to database: {error_message}")


def extract_databse(db_name: str, table_name: str) -> DataFrame | None:
    """
    Extract data from a specified PostgreSQL database table into a DataFrame.

    This function establishes a connection to a PostgreSQL database using
    environment variables for connection details and retrieves data from
    the specified table. It returns the data as a Spark DataFrame or None
    if an error occurs during the extraction process.

    Args:
        db_name (str): The name of the database to connect to.
        table_name (str): The name of the table to extract data from.

    Returns:
        DataFrame | None: A DataFrame containing the table data, or None if an error occurs.
    """
    spark = init_spark_session()

    # set variable for database
    HOST = os.getenv("POSTGRES_HOST")
    PORT = os.getenv("POSTGRES_PORT")
    USER = os.getenv("POSTGRES_USER")
    PASSWORD = os.getenv("POSTGRES_PASSWORD")

    DB_URL = f"jdbc:postgresql://{HOST}:{PORT}/{db_name}"
    # set config
    jdbc_url = DB_URL
    connection_properties = {
        "user": USER,
        "password": PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_properties,  # type: ignore
        )

        return df

    except Exception as e:
        error_message = str(e)
        print(f"Error connecting to database: {error_message}")
