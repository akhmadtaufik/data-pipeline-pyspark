from typing import List
from pyspark.sql import DataFrame
from src.utils.spark_session import init_spark_session
from src.utils.log_message import log_operation
from src.utils.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    DB_STAGING,
)


def extract_staging(table_name: str) -> DataFrame | None:
    """
    Extracts data from a staging area in a PostgreSQL database into a Spark DataFrame.

    This function initializes a SparkSession and attempts to read data from the specified
    staging area using JDBC. It logs the operation's success or failure, including any
    error messages encountered during the extraction process.

    Parameters:
    - table_name (str): The name of the staging table to extract data from.

    Returns:
    - DataFrame | None: A Spark DataFrame containing the extracted data if successful,
      otherwise None if an error occurs.
    """
    spark = init_spark_session()

    try:
        df = spark.read.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_STAGING}",
            table=f"staging.{table_name}",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )

        log_operation(
            step="extract",
            process="warehouse",
            status="success",
            source="staging area",
            table_name=table_name,
            message=f"Berhasil ekstrak {table_name} dari staging area",
        )

        return df

    except Exception as e:
        log_operation(
            step="extract",
            process="warehouse",
            status="failed",
            source="staging area",
            table_name=table_name,
            error_msg=str(e),
        )
