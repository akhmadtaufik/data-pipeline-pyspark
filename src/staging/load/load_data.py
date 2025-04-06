import traceback
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Any
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


def check_table_status(schema: str, table_name: str) -> tuple[Any, Any, Any]:
    """
    Checks the status of a specified table in the database.

    This function connects to a PostgreSQL database and checks if a specified table exists
    within a given schema. It also checks for the existence of an 'etl_date' column in the table
    and retrieves the maximum value of 'etl_date' if the column exists. The function logs the
    status of these checks using the organization's logging system.

    Parameters:
    - schema (str): The schema name where the table is located.
    - table_name (str): The name of the table to check.

    Returns:
    - tuple[Any, Any, Any]: A tuple containing:
      - A boolean indicating if the table exists.
      - A boolean indicating if the 'etl_date' column exists.
      - The maximum 'etl_date' value if the column exists, otherwise None.
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=DB_STAGING,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    table_exists = False
    etl_date_exists = False
    max_etl_date = None

    try:
        with conn.cursor() as cursor:
            # Check table existence
            cursor.execute(
                f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                )
            """
            )
            table_exists = cursor.fetchone()[0]  # type: ignore

            if not table_exists:
                log_operation(
                    step="status_check",
                    process="staging",
                    status="info",
                    source="system",
                    table_name=table_name,
                    message=f"Tabel {schema}.{table_name} tidak ditemukan",
                )
                return (False, False, None)

            # Check etl_date column existence
            cursor.execute(
                f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                    AND column_name = 'etl_date'
                )
            """
            )
            etl_date_exists = cursor.fetchone()[0]  # type: ignore

            # Get max etl_date if column exists
            if etl_date_exists:
                cursor.execute(
                    f"""
                    SELECT MAX(etl_date) FROM {schema}.{table_name}
                """
                )
                max_etl_date = cursor.fetchone()[0]  # type: ignore
                log_operation(
                    step="status_check",
                    process="staging",
                    status="info",
                    source="system",
                    table_name=table_name,
                    message=f"Max etl_date: {max_etl_date}",
                )

    except Exception as e:
        log_operation(
            step="status_check",
            process="staging",
            status="error",
            source="system",
            table_name=table_name,
            error_msg=str(e),
        )
        raise
    finally:
        conn.close()

    return (table_exists, etl_date_exists, max_etl_date)


def full_load_with_spark(df: DataFrame, schema: str, table_name: str) -> None:
    """
    Performs a full load of the given DataFrame into a PostgreSQL database table.

    This function writes the DataFrame to a specified schema and table in a PostgreSQL
    database using JDBC. It overwrites any existing data in the table. After the load
    operation, it logs the success or failure of the operation using the `log_operation`
    function.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to be loaded into the database.
    schema : str
        The schema in the PostgreSQL database where the table resides.
    table_name : str
        The name of the table in the PostgreSQL database.

    Raises
    ------
    Exception
        If the load operation fails, the exception is logged and re-raised.
    """
    try:
        df.write.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_STAGING}",
            table=f"{schema}.{table_name}",
            mode="overwrite",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )
        log_operation(
            step="full_load",
            process="staging",
            status="success",
            source="spark",
            table_name=table_name,
            message=f"Berhasil full load {df.count()} records",
        )
    except Exception as e:
        log_operation(
            step="full_load",
            process="staging",
            status="failed",
            source="spark",
            table_name=table_name,
            error_msg=str(e),
        )
        raise


def incremental_load_with_pandas(
    df: DataFrame,
    schema: str,
    table_name: str,
    max_etl_date: datetime,
    source: str,
) -> None:
    """
    Performs an incremental data load from a Spark DataFrame to a PostgreSQL table.

    This function filters the input DataFrame for records with an 'etl_date' greater
    than the specified 'max_etl_date', converts it to a Pandas DataFrame, and loads
    it into a temporary PostgreSQL table. It then performs an UPSERT operation to
    insert or update records in the target table based on the 'id' column. The
    function logs each step of the process, including skipped loads, successful
    writes, and errors.

    Parameters:
    - df (DataFrame): The Spark DataFrame containing the data to be loaded.
    - schema (str): The schema of the target PostgreSQL table.
    - table_name (str): The name of the target PostgreSQL table.
    - max_etl_date (datetime): The maximum ETL date to filter records.
    - source (str): The source identifier for logging purposes.

    Returns:
    - None

    Raises:
    - Exception: If any error occurs during the load process.
    """
    full_table = f"{schema}.{table_name}"
    temp_table = f"temp_{table_name}"

    try:
        # Convert and filter data
        pandas_df = df.filter(f"etl_date > '{max_etl_date}'").toPandas()  # type: ignore

        if pandas_df.empty:  # type: ignore
            log_operation(
                step="incremental_load",
                process="staging",
                status="skipped",
                source=source,
                table_name=table_name,
                message="Tidak ada data baru untuk di-load",
            )
            return

        # Write to temporary table
        pandas_df.to_sql(  # type: ignore
            name=temp_table, con=engine, if_exists="replace", index=False
        )
        log_operation(
            step="incremental_load",
            process="staging",
            status="info",
            source=source,
            table_name=table_name,
            message=f"Berhasil menulis {len(pandas_df)} records ke tabel temp",  # type: ignore
        )

        # Execute UPSERT
        with engine.connect() as conn:
            with conn.begin():
                upsert_sql = f"""
                    INSERT INTO {full_table}
                    SELECT * FROM {temp_table}
                    ON CONFLICT (id)
                    DO UPDATE SET
                        updated_at = EXCLUDED.updated_at,
                        etl_date = EXCLUDED.etl_date
                """
                conn.execute(text(upsert_sql))
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

        log_operation(
            step="incremental_load",
            process="staging",
            status="success",
            source=source,
            table_name=table_name,
            message=f"Berhasil upsert {len(pandas_df)} records",  # type: ignore
        )

    except Exception as e:
        log_operation(
            step="incremental_load",
            process="staging",
            status="failed",
            source=source,
            table_name=table_name,
            error_msg=str(e),
        )
        raise


def load_data_to_staging(
    df: DataFrame, target_table: str, source: str, schema: str = "staging"
) -> None:
    """
    Loads data into a staging table in a PostgreSQL database with a dynamic strategy.

    This function determines the appropriate loading strategy (incremental or full load)
    based on the existence of the target table and the 'etl_date' column. It logs each
    step of the process, including validation, strategy decision, and completion. In case
    of errors, it attempts to back up the data and logs the error details.

    Parameters:
    - df (DataFrame): The DataFrame containing the data to be loaded.
    - target_table (str): The name of the target table in the database.
    - source (str): The source identifier for logging purposes.
    - schema (str, optional): The schema of the target table, default is "staging".

    Returns:
    - None

    Raises:
    - ValueError: If the DataFrame is None or empty.
    - Exception: If any error occurs during the load process.
    """
    try:
        log_operation(
            step="load_init",
            process="staging",
            status="started",
            source=source,
            table_name=target_table,
            message="Memulai proses loading data",
        )

        # Validate DataFrame
        if df is None:
            log_operation(
                step="validation",
                process="staging",
                status="failed",
                source=source,
                table_name=target_table,
                error_msg="DataFrame is None",
            )
            raise ValueError("DataFrame tidak valid")

        if df.count() == 0:
            log_operation(
                step="validation",
                process="staging",
                status="skipped",
                source=source,
                table_name=target_table,
                message="DataFrame kosong",
            )
            return

        # Check table status
        table_exists, etl_date_exists, max_etl_date = check_table_status(
            schema, target_table
        )

        # Load strategy decision
        if table_exists and etl_date_exists and max_etl_date is not None:
            log_operation(
                step="load_strategy",
                process="staging",
                status="info",
                source=source,
                table_name=target_table,
                message="Memulai incremental load",
            )
            incremental_load_with_pandas(
                df, schema, target_table, max_etl_date, source
            )
        else:
            log_operation(
                step="load_strategy",
                process="staging",
                status="info",
                source=source,
                table_name=target_table,
                message="Memulai full load",
            )
            full_load_with_spark(df, schema, target_table)

        log_operation(
            step="load_complete",
            process="staging",
            status="success",
            source=source,
            table_name=target_table,
            message="Proses load selesai",
        )

    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"

        log_operation(
            step="load_error",
            process="staging",
            status="failed",
            source=source,
            table_name=target_table,
            error_msg=error_msg,
            message="Proses load gagal",
        )

        # Backup failed data
        try:
            handle_error(df, target_table, e, source)  # type: ignore
            log_operation(
                step="backup",
                process="staging",
                status="success",
                source=source,
                table_name=target_table,
                message="Backup data gagal berhasil",
            )
        except Exception as backup_error:
            log_operation(
                step="backup",
                process="staging",
                status="failed",
                source=source,
                table_name=target_table,
                error_msg=str(backup_error),
                message="Gagal melakukan backup",
            )

        raise

    finally:
        # Cleanup temporary tables
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS temp_{target_table}"))
            log_operation(
                step="cleanup",
                process="staging",
                status="info",
                source=source,
                table_name=target_table,
                message="Berhasil membersihkan tabel temp",
            )
        except Exception as e:
            log_operation(
                step="cleanup",
                process="staging",
                status="warning",
                source=source,
                table_name=target_table,
                error_msg=str(e),
                message="Gagal membersihkan tabel temp",
            )
