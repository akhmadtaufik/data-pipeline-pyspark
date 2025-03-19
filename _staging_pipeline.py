import logging
from typing import List, Any
from pyspark.sql import DataFrame
from src.utils.log_message import log_operation
from src.utils.config import DB_STAGING
from src.staging.extract.extract_csv import extract_csv
from src.staging.extract.extract_api import extract_api
from src.staging.extract.extract_database import (
    extract_table_name,
    extract_databse,
)
from src.staging.load.load_data import load_data_to_staging


def run_pipeline():
    """
    Runs the ETL pipeline to extract data from CSV files, a database, and an API,
    and then loads the data into a staging area.

    The function performs the following steps:
    1. Extracts data from specified CSV files and logs the operation status.
    2. Extracts table names from a PostgreSQL database and retrieves data from each table,
       logging the operation status.
    3. Extracts data from an API within a specified date range and logs the operation status.
    4. Loads the extracted data from CSV files, database tables, and API into a staging area,
       logging the operation status.

    Logs are generated for each step to track the success or failure of the operations.
    In case of errors, the function logs the error details and raises exceptions.
    """
    try:
        # --------------- Extract From Source---------------#
        # 1. Extract CSV
        csv_tables = {
            "people": "data/raw/people.csv",
            "relationships": "data/raw/relationships.csv",
        }

        for table, path in csv_tables.items():
            try:
                df: DataFrame = extract_csv(path)
                log_operation(
                    "extract",
                    "success",
                    "csv",
                    table,
                    message="Data CSV berhasil diekstrak",
                )

            except Exception as e:
                log_operation(
                    "extract", "failed", "csv", table, error_msg=str(e)
                )

        # 2. Extract Database
        db_name = str(DB_STAGING)
        try:
            tables: List[Any] = extract_table_name(db_name)  # type: ignore
            log_operation(
                "extract",
                "info",
                "database",
                "system",
                message=f"Found {len(tables)} tables in database",  # type: ignore
            )

            for table in tables:  # type: ignore
                try:
                    df: DataFrame = extract_databse(db_name, table)  # type: ignore
                    log_operation(
                        "extract",
                        "success",
                        "database",
                        table,
                        "Table berhasil diekstrak",
                    )

                except Exception as e:
                    log_operation(
                        "extract",
                        "failed",
                        "database",
                        table,
                        error_msg=str(e),
                    )
                    continue

        except Exception as e:
            log_operation(
                "extract", "failed", "database", "system", error_msg=str(e)
            )
            raise

        # 3. Extract API
        start_date = "2000-01-01"
        end_date = "2010-12-31"

        try:
            milestones_df: DataFrame = extract_api(start_date, end_date)

            if milestones_df.isEmpty():
                log_operation(
                    "extract",
                    "failed",
                    "api",
                    "milestones",
                    message="Tidak ada data dari API",
                )

            else:
                log_operation(
                    "extract",
                    "success",
                    "api",
                    "milestones",
                    message=f"Data API berhasil ({milestones_df.count()} records)",
                )

        except Exception as e:
            log_operation(
                "extract", "failed", "api", "milestones", error_msg=str(e)
            )
            raise

        # --------------- Load To Staging ---------------#
        log_operation(
            "load",
            "started",
            "system",
            "system",
            message="Memulai proses loading",
        )
        # 1. Load CSV ke Staging
        for table in csv_tables.keys():
            try:
                load_data_to_staging(locals()[f"{table}_df"], table, "csv")

                log_operation("load", "success", "csv", table)

            except Exception as e:
                log_operation("load", "failed", "csv", table, error_msg=str(e))
                raise

        # 2. Load Database ke Staging
        for table in tables:  # type: ignore
            try:
                if f"df_{table}" in locals():
                    load_data_to_staging(
                        locals()[f"df_{table}"], table, "database"
                    )
                    log_operation("load", "success", "database", table)

            except Exception as e:
                log_operation(
                    "load", "failed", "database", table, error_msg=str(e)
                )
                continue

        # 3. Load API ke Staging
        try:
            if not milestones_df.isEmpty():
                load_data_to_staging(milestones_df, "milestones", "api")
                log_operation("load", "success", "api", "milestones")

        except Exception as e:
            log_operation(
                "load", "failed", "api", "milestones", error_msg=str(e)
            )
            raise

        log_operation(
            "complete",
            "success",
            "system",
            "system",
            message="Pipeline selesai",
        )

    except Exception as e:
        log_operation("system", "failed", "system", "system", error_msg=str(e))
        logging.error(f"❌ FATAL ERROR: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pipeline()
