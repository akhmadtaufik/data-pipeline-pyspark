import traceback
import psycopg2
from datetime import datetime
from pyspark.sql import DataFrame, functions as F
from src.warehouse.load.handle_error import handle_warehouse_error
from src.utils.log_message import log_operation
from src.utils.spark_session import init_spark_session
from src.utils.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    DB_WAREHOUSE,
)


def check_table_max_created_at(table_name: str) -> datetime | None:
    """
    Retrieves the maximum 'created_at' timestamp from a specified table in the warehouse database.

    This function connects to the warehouse database and executes a query to find the maximum
    'created_at' timestamp from the specified table. If an error occurs during the database
    operation, it logs the error and returns None.

    Parameters:
    - table_name (str): The name of the table to query.

    Returns:
    - datetime | None: The maximum 'created_at' timestamp if successful, otherwise None.
    """
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=DB_WAREHOUSE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT MAX(created_at) FROM warehouse.{table_name}"
            )
            max_date = cursor.fetchone()[0]  # type: ignore

        return max_date

    except psycopg2.Error as e:
        log_operation(
            step="status_check",
            process="warehouse",
            status="error",
            source="system",
            table_name=table_name,
            error_msg=str(e),
        )

        return None

    finally:
        conn.close()


def full_load(df: DataFrame, table_name: str) -> None:
    """
    Performs a full load of data into a specified warehouse table.

    This function truncates the specified table and its dependencies using the CASCADE option,
    then loads new data from a DataFrame into the table using append mode. It logs the success
    or failure of each operation step using the `log_operation` function.

    Parameters:
    - df (DataFrame): The DataFrame containing the data to be loaded.
    - table_name (str): The name of the table to be truncated and loaded.

    Raises:
    - psycopg2.Error: If a database error occurs during the truncate operation.
    - Exception: If any other error occurs during the full load process.

    Note:
    - The function uses a PostgreSQL connection to execute the truncate operation.
    - The DataFrame is written to the database using JDBC.
    """
    conn = None
    try:
        # Step 1: Truncate table and dependencies using CASCADE
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=DB_WAREHOUSE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        conn.autocommit = True  # Enable autocommit for DDL

        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE warehouse.{table_name} CASCADE;")

        log_operation(
            step="truncate",
            process="warehouse",
            status="success",
            source="system",
            table_name=table_name,
            message=f"Truncated {table_name} with CASCADE",
        )

        # Step 2: Load data using append mode
        df.write.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_WAREHOUSE}",
            table=f"warehouse.{table_name}",
            mode="append",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )

        log_operation(
            step="full_load",
            process="warehouse",
            status="success",
            source=f"transformation {table_name}",
            table_name=table_name,
            message=f"Full load berhasil: {table_name}",
        )

    except psycopg2.Error as e:
        log_operation(
            step="truncate",
            process="warehouse",
            status="failed",
            source="system",
            table_name=table_name,
            error_msg=str(e),
        )
        raise

    except Exception as e:
        log_operation(
            step="full_load",
            process="warehouse",
            status="failed",
            source=f"transformation {table_name}",
            table_name=table_name,
            error_msg=str(e),
        )
        raise

    finally:
        if conn:
            conn.close()


def incremental_load(
    df: DataFrame, table_name: str, max_date: datetime
) -> None:
    """
    Performs an incremental load of new data into a specified warehouse table.

    This function filters the input DataFrame to include only records with a
    'created_at' timestamp greater than the provided 'max_date'. If no new
    records are found, it logs the operation as skipped. Otherwise, it appends
    the new records to the specified table in the warehouse database and logs
    the operation as successful. In case of an error, it logs the operation as
    failed and raises the exception.

    Parameters:
    - df (DataFrame): The DataFrame containing the data to be loaded.
    - table_name (str): The name of the warehouse table to load data into.
    - max_date (datetime): The maximum date to filter records by.

    Returns:
    - None
    """
    try:
        filtered_df = df.filter(F.col("created_at") > max_date)  # type: ignore

        if filtered_df.count() == 0:
            log_operation(
                step="incremental_load",
                process="warehouse",
                status="skipped",
                source=f"transformation {table_name}",
                table_name=table_name,
                message="Tidak ada data baru",
            )
            return

        filtered_df.write.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_WAREHOUSE}",
            table=f"warehouse.{table_name}",
            mode="append",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )

        log_operation(
            step="incremental_load",
            process="warehouse",
            status="success",
            source=f"transformation {table_name}",
            table_name=table_name,
            message=f"Incremental load berhasil: {filtered_df.count()} records",
        )

    except Exception as e:
        log_operation(
            step="incremental_load",
            process="warehouse",
            status="failed",
            source=f"transformation {table_name}",
            table_name=table_name,
            error_msg=str(e),
        )
        raise


def load_warehouse(df: DataFrame, table_name: str) -> None:
    """
    Loads data from a DataFrame into a warehouse table, performing either a full or incremental load.

    This function first checks if the DataFrame is empty and logs the operation as skipped if true.
    If the DataFrame is not empty, it retrieves the maximum 'created_at' timestamp from the target
    table to determine whether to perform an incremental or full load. In case of an error during
    the load process, it logs the error and attempts to back up the DataFrame to MinIO.

    Parameters
    ----------
    df : DataFrame
        The DataFrame containing the data to be loaded.
    table_name : str
        The name of the target table in the warehouse.

    Raises
    ------
    Exception
        If an error occurs during the load operation, it logs the error, handles it, and re-raises
        the exception.
    """
    try:
        if df.count() == 0:
            log_operation(
                step="validation",
                process="warehouse",
                status="skipped",
                source=f"transformation {table_name}",
                table_name=table_name,
                message="DataFrame kosong",
            )
            return

        max_created_at = check_table_max_created_at(table_name)

        if max_created_at:
            incremental_load(df, table_name, max_created_at)
        else:
            full_load(df, table_name)

    except Exception as e:
        log_operation(
            step="load_error",
            process="warehouse",
            status="failed",
            source=f"transformation {table_name}",
            table_name=table_name,
            error_msg=str(e),
            message="Proses load gagal",
        )

        handle_warehouse_error(df, table_name, e, "warehouse")

        raise
