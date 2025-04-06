from typing import List
from pyspark.sql import DataFrame
from src.utils.spark_session import init_spark_session
from src.utils.log_message import log_operation
from src.utils.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


def extract_table_name(db_name: str) -> List[str] | None:
    """
    Extracts table names from a specified PostgreSQL database.

    This function connects to a PostgreSQL database using JDBC, retrieves the
    names of all tables in the public schema, and logs the operation details.
    If an error occurs during the extraction, it logs the error and raises an
    exception.

    Args:
        db_name (str): The name of the database from which to extract table names.

    Returns:
        List[str] | None: A list of table names if successful, or None if an error occurs.

    Raises:
        Exception: If there is an error during the extraction process.
    """
    spark = init_spark_session()

    try:
        tables_df: DataFrame = spark.read.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}",
            table="(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') as all_tables",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )

        table_names: List[str] = [
            str(row.table_name) for row in tables_df.collect()
        ]

        log_operation(
            step="extract",
            process="staging",
            status="info",
            source="database",
            table_name="system",
            message=f"Menemukan {len(table_names)} tabel di database {db_name}",
        )

        return table_names

    except Exception as e:
        log_operation(
            step="extract",
            process="staging",
            status="failed",
            source="database",
            table_name="system",
            error_msg=str(e),
        )
        raise


def extract_database(db_name: str, table_name: str) -> DataFrame:
    """
    Extracts data from a specified PostgreSQL database table into a Spark DataFrame.

    This function initializes a SparkSession and attempts to read data from a
    PostgreSQL database table using JDBC. It logs the operation status using
    the organization's logging utility. If the extraction is successful, it
    returns the DataFrame; otherwise, it logs the error and raises an exception.

    Args:
        db_name (str): The name of the database to connect to.
        table_name (str): The name of the table to extract data from.

    Returns:
        DataFrame | None: A Spark DataFrame containing the extracted data, or
        None if the extraction fails.
    """
    spark = init_spark_session()

    try:
        df: DataFrame = spark.read.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}",
            table=table_name,
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )

        log_operation(
            step="extract",
            process="staging",
            status="success",
            source="database",
            table_name=table_name,
            message=f"Berhasil ekstrak {table_name} dari {db_name}",
        )

        return df

    except Exception as e:
        log_operation(
            step="extract",
            process="staging",
            status="failed",
            source="database",
            table_name=table_name,
            error_msg=str(e),
        )

        raise
