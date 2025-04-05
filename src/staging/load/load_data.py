import traceback
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from pyspark.sql import DataFrame
from src.staging.load.handle_error import handle_error
from src.utils.log_message import log_operation
from src.utils.spark_session import init_spark_session
from src.utils.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    DB_STAGING,
)

# Initialize Spark and SQLAlchemy Engine
spark = init_spark_session()
engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_STAGING}"
)


def get_last_etl_date(schema: str, table_name: str) -> datetime:
    """
    Retrieves the last ETL run date from the staging table.
    If the table does not exist or the etl_date column is empty,
    it returns a default date (January 1, 1100).

    Args:
        schema (str): The schema name (e.g., 'staging').
        table_name (str): The target table name.

    Returns:
        datetime: The last ETL run date or the default date if not found.
    """
    spark = init_spark_session()
    full_table_name = f"{schema}.{table_name}"
    default_date = datetime(1100, 1, 1)

    try:
        query = f"(SELECT MAX(etl_date) as last_etl_date FROM {full_table_name}) as t"

        df = spark.read.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_STAGING}",
            table=query,
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )

        # If the query returns data and the value is not null, return that value
        if df.count() > 0:
            row = df.collect()[0]
            if row["last_etl_date"] is not None:
                return row["last_etl_date"]

        # If the etl_date is not found, log a warning and return the default_date
        log_operation(
            step="load",
            process="staging",
            status="warning",
            source="system",
            table_name=table_name,
            message=f"No etl_date found in table {full_table_name}, using default date {default_date}",
        )
        return default_date

    except Exception as e:
        # Log the error and return the default_date if an exception occurs (e.g., table not existing)
        log_operation(
            step="load",
            process="staging",
            status="warning",
            source="system",
            table_name=table_name,
            error_msg=str(e),
            message=f"Failed to retrieve last_etl_date, using default date {default_date}",
        )
        return default_date


def load_data_to_staging(
    df: DataFrame, target_table: str, source: str, schema: str = "staging"
) -> None:
    """
    Loads data from a DataFrame into a staging table in a PostgreSQL database.

    This function filters the DataFrame for new data based on the last ETL date,
    writes the filtered data to a temporary table, and performs an upsert operation
    into the target staging table. It logs the operation status and handles any
    errors that occur during the process.

    Args:
        df (DataFrame): The DataFrame containing the data to be loaded.
        target_table (str): The name of the target table in the staging schema.
        source (str): The source of the data being loaded.
        schema (str, optional): The schema where the target table resides. Defaults to "staging".

    Raises:
        Exception: If an error occurs during the data loading process.
    """
    full_table_name = f"{schema}.{target_table}"
    temp_table_name = f"temp_{target_table}"

    # Initialize new_data to prevent errors if exception happens early
    new_data = df

    try:
        log_operation(
            step="load",
            process="staging",
            status="started",
            source=source,
            table_name=target_table,
            message="Memulai proses load",
        )

        # Check if DataFrame is not None and not empty
        if df is None:
            raise ValueError("DataFrame is None")

        if df.rdd.isEmpty():
            log_operation(
                step="load",
                process="staging",
                status="skipped",
                source=source,
                table_name=target_table,
                message="DataFrame kosong, tidak ada data untuk di-load",
            )
            return

        # Get and filter data using SQL query syntax
        last_etl_date: datetime = get_last_etl_date(schema, target_table)
        date_filter: str = last_etl_date.strftime("%Y-%m-%d %H:%M:%S")

        if "etl_date" in df.columns:
            new_data = df.filter(f"etl_date > '{date_filter}'")  # type: ignore

            # Check if filtered data is empty
            if new_data.rdd.isEmpty():
                log_operation(
                    step="load",
                    process="staging",
                    status="skipped",
                    source=source,
                    table_name=target_table,
                    message=f"Tidak ada data baru setelah {date_filter}",
                )
                return
        else:
            new_data = df
            log_operation(
                step="load",
                process="staging",
                status="info",
                source=source,
                table_name=target_table,
                message="Kolom etl_date tidak ditemukan, menggunakan semua data",
            )

        # Write to temporary table
        new_data.write.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_STAGING}",
            table=temp_table_name,
            mode="overwrite",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )

        # Execute upsert
        try:
            spark.sql(  # type: ignore
                f"""
                INSERT INTO {full_table_name}
                SELECT * FROM {temp_table_name}
                ON CONFLICT (id)
                DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    etl_date = EXCLUDED.etl_date
            """
            )
        except Exception as sql_error:
            # Provide more specific error information for SQL errors
            log_operation(
                step="load",
                process="staging",
                status="failed",
                source=source,
                table_name=target_table,
                error_msg=f"SQL Error: {str(sql_error)}",
            )
            raise

        log_operation(
            step="load",
            process="staging",
            status="success",
            source=source,
            table_name=target_table,
            message=f"Data berhasil di-load ({new_data.count()} records)",
        )

    except Exception as e:
        error_msg = f"Error: {str(e)}\n{traceback.format_exc()}"

        try:
            handle_error(new_data, target_table, e, source)
        except Exception as backup_error:
            log_operation(
                step="backup",
                process="staging",
                status="failed",
                source=source,
                table_name=target_table,
                error_msg=f"Backup error: {str(backup_error)}",
            )

        log_operation(
            step="load",
            process="staging",
            status="failed",
            source=source,
            table_name=target_table,
            error_msg=error_msg,
        )

        # Re-raise the exception to let the pipeline know something went wrong
        raise

    finally:
        # Ensure temp table is dropped even if an error occurs
        try:
            if spark.catalog.tableExists(temp_table_name):
                spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")  # type: ignore
        except Exception as drop_error:
            log_operation(
                step="load",
                process="staging",
                status="warning",
                source=source,
                table_name=target_table,
                message=f"Gagal menghapus tabel temp: {str(drop_error)}",
            )
