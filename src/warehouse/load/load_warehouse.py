from numpy import full
import psycopg2
from datetime import datetime
from typing import List
from pyspark.sql import DataFrame, functions as F
from src.warehouse.load.handle_error import handle_warehouse_error
from src.utils.log_message import log_operation
from src.utils.spark_session import init_spark_session
from src.utils.spark_connection import write_jdbc, read_jdbc
from src.utils.psycopg2_connection import read_db
from src.utils.config import DB_WAREHOUSE


def check_table_max_created_at(table_name: str) -> datetime | None:
    """
    Retrieves the maximum 'created_at' timestamp from a specified table in the warehouse.

    Args:
        table_name (str): The name of the table to query.

    Returns:
        datetime | None: The maximum 'created_at' timestamp if found, otherwise None.

    Logs:
        Logs an error message if a database error occurs during the operation.
    """
    try:
        conn = read_db(db_name=str(DB_WAREHOUSE))

        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT MAX(created_at) FROM warehouse.{table_name} WHERE created_at IS NOT NULL"
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
        conn = read_db(db_name=str(DB_WAREHOUSE))
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
        write_jdbc(
            df, str(DB_WAREHOUSE), schema="warehouse", table_name=table_name
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

        write_jdbc(
            df=filtered_df,
            db=str(DB_WAREHOUSE),
            schema="warehouse",
            table_name=table_name,
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


def load_warehouse(
    df: DataFrame,
    table_name: str,
    mode: str = "auto",
    composite_key_cols: List[str] = None,  # type: ignore
) -> None:
    try:
        # Validate DataFrame before processing
        if df is None or df.count() == 0:
            log_operation(
                step="validation",
                process="warehouse",
                status="skipped",
                source=f"transformation {table_name}",
                table_name=table_name,
                message="DataFrame kosong",
            )
            return

        # Determine primary key column for specific tables if upsert mode
        upsert_key = None

        # Dimension tables
        if table_name == "dim_company":
            upsert_key = "object_id_nk"
        elif table_name == "dim_fund":
            upsert_key = "fund_id_nk"
        elif table_name == "dim_investor":
            upsert_key = "object_id_nk"
        elif table_name == "dim_location":
            upsert_key = "office_id_nk"
        elif table_name == "dim_milestone_type":
            upsert_key = "milestone_code"
        elif table_name == "dim_person":
            upsert_key = "people_id_nk"
        elif table_name == "dim_relationship_type":
            upsert_key = "title"
        elif table_name == "dim_round_type":
            upsert_key = "funding_round_type"

        # Fact tables
        elif table_name == "fact_acquisition":
            upsert_key = "acquisition_id_nk"
        elif table_name == "fact_funding_round":
            upsert_key = "funding_round_id_nk"
        elif table_name == "fact_investment":
            upsert_key = "investment_id_nk"
        elif table_name == "fact_ipo":
            upsert_key = "ipo_id_nk"
        elif table_name == "fact_milestone":
            upsert_key = "milestone_id_nk"
        elif table_name == "fact_relationship":
            upsert_key = "relationship_id_nk"

        spark = init_spark_session()

        if mode == "full":
            full_load(df, table_name)

        elif mode == "upsert" and upsert_key:
            # Standard upsert using the write_jdbc utility with upsert mode
            write_jdbc(
                df=df,
                db=str(DB_WAREHOUSE),
                schema="warehouse",
                table_name=table_name,
                mode="upsert",
                upsert_key=upsert_key,
            )

            log_operation(
                step="upsert_load",
                process="warehouse",
                status="success",
                source=f"transformation {table_name}",
                table_name=table_name,
                message=f"Upsert berhasil dengan {df.count()} records",
            )

        elif (
            mode == "dedup"
            and composite_key_cols
            and len(composite_key_cols) > 0
        ):
            # Handle tables without single upsert key but with composite keys for uniqueness

            # Load existing data from target table
            try:
                existing_df = read_jdbc(
                    db=str(DB_WAREHOUSE),
                    schema="warehouse",
                    table_name=table_name,
                )

                # Register DataFrames as temp views
                df.createOrReplaceTempView("new_data")
                existing_df.createOrReplaceTempView("existing_data")

                # Construct join condition for composite key
                join_condition = " AND ".join(
                    [
                        f"new.{col} = existing.{col}"
                        for col in composite_key_cols
                    ]
                )

                # Use anti-join to find records that don't exist yet
                deduplicated_df = spark.sql(  # type: ignore
                    f"""
                    SELECT new.*
                    FROM new_data new
                    LEFT JOIN existing_data existing
                        ON {join_condition}
                    WHERE {" OR ".join([f"existing.{col} IS NULL" for col in composite_key_cols])}
                """
                )

                if deduplicated_df.count() == 0:
                    log_operation(
                        step="dedup_load",
                        process="warehouse",
                        status="skipped",
                        source=f"transformation {table_name}",
                        table_name=table_name,
                        message="Tidak ada data baru untuk dimuat setelah deduplikasi",
                    )
                    return

                # Append only the new, deduplicated records
                write_jdbc(
                    df=deduplicated_df,
                    db=str(DB_WAREHOUSE),
                    schema="warehouse",
                    table_name=table_name,
                    mode="append",
                )

                log_operation(
                    step="dedup_load",
                    process="warehouse",
                    status="success",
                    source=f"transformation {table_name}",
                    table_name=table_name,
                    message=f"Dedup load berhasil: {deduplicated_df.count()} records ditambahkan",
                )

            except Exception as e:
                # If target table doesn't exist yet, do a full load
                log_operation(
                    step="dedup_load",
                    process="warehouse",
                    status="info",
                    source=f"transformation {table_name}",
                    table_name=table_name,
                    message=f"Tidak dapat membaca tabel target untuk deduplikasi: {str(e)}. Melakukan full load.",
                )
                full_load(df, table_name)

        elif mode == "incremental" or (
            mode == "auto" and check_table_max_created_at(table_name)
        ):
            # Timestamp-based incremental load
            max_created_at = check_table_max_created_at(table_name)

            if max_created_at:
                # Filter data newer than the last load timestamp
                filtered_df = df.filter(F.col("created_at") > max_created_at)  # type: ignore

                if filtered_df.count() == 0:
                    log_operation(
                        step="incremental_load",
                        process="warehouse",
                        status="skipped",
                        source=f"transformation {table_name}",
                        table_name=table_name,
                        message="Tidak ada data baru untuk dimuat",
                    )
                    return

                # Write only the new data to the target table
                write_jdbc(
                    df=filtered_df,
                    db=str(DB_WAREHOUSE),
                    schema="warehouse",
                    table_name=table_name,
                    mode="append",
                )

                log_operation(
                    step="incremental_load",
                    process="warehouse",
                    status="success",
                    source=f"transformation {table_name}",
                    table_name=table_name,
                    message=f"Incremental load berhasil: {filtered_df.count()} records ditambahkan",
                )

            else:
                # Fall back to full load if no timestamp found
                full_load(df, table_name)

        else:
            # Default to full load if no valid mode is specified or table doesn't exist
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
