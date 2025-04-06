import time
from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from src.utils.log_message import log_operation
from src.utils.config import DB_STAGING, DB_SOURCE
from src.utils.spark_session import init_spark_session
from src.staging.extract.extract_csv import extract_csv
from src.staging.extract.extract_api import extract_api
from src.staging.extract.extract_database import (
    extract_table_name,
    extract_database,
)
from src.staging.load.load_data import load_data_to_staging


def run_pipeline() -> None:
    """
    Executes the data pipeline process, extracting data from various sources and loading it into the staging area.

    This function initializes a SparkSession and performs the following steps:
    1. Extracts data from CSV files and a PostgreSQL database.
    2. Extracts data from an API within a specified date range.
    3. Loads the extracted data into the staging area using the organization's logging utility.

    The function handles exceptions during extraction and loading, logging any errors encountered.
    Finally, it ensures the SparkSession is stopped, with a fallback to forcefully terminate it if necessary.

    Returns:
        None
    """
    spark: Optional[SparkSession] = None

    try:
        # Initialize Spark session that will be used throughout the pipeline
        spark = init_spark_session()

        # --------------- Extract From Source---------------#
        # 1. Extract CSV
        people_df = extract_csv("data/raw/people.csv")
        relations_df = extract_csv("data/raw/relationships.csv")

        # 2. Extract Database
        db_name = str(DB_SOURCE)
        try:
            tables = extract_table_name(db_name)
            log_operation(
                step="extract",
                process="staging",
                status="info",
                source="database",
                table_name="system",
                message=f"Memproses {len(tables)} tabel dari {db_name}",  # type: ignore
            )

            dataframes: Dict[str, DataFrame] = {}
            for table in tables:  # type: ignore
                df = extract_database(db_name, table)
                dataframes[table] = df

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

        # 3. Extract API
        start_date = "2000-01-01"
        end_date = "2010-12-31"

        milestones_df: DataFrame = extract_api(start_date, end_date)

        # --------------- Load To Staging ---------------#
        # 1. Load CSV ke Staging
        load_data_to_staging(people_df, "people", "csv")  # type: ignore
        load_data_to_staging(relations_df, "relationships", "csv")  # type: ignore

        # 2. Load Database ke Staging
        for table, df in dataframes.items():
            load_data_to_staging(df, table, "database")

        # 3. Load API ke Staging
        load_data_to_staging(milestones_df, "milestones", "api")

    except Exception as e:
        pass

    finally:
        if spark:
            try:
                time.sleep(5)
                spark.stop()
                print("SparkSession berhasil dihentikan")
            except Exception as stop_error:
                print(f"Gagal menghentikan SparkSession: {str(stop_error)}")
                # Force kill jika diperlukan (Windows)
                import os

                os.system("taskkill /F /IM java.exe")


if __name__ == "__main__":
    run_pipeline()
