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
    Runs the data pipeline to extract data from various sources and load it into the staging area.

    This function orchestrates the extraction of data from CSV files, a PostgreSQL database,
    and an API, followed by loading the extracted data into the staging tables. It handles
    any exceptions that occur during the process.

    Raises:
        Exception: If an error occurs during any stage of the pipeline.
    """
    try:
        # --------------- Extract From Source---------------#
        # 1. Extract CSV
        people_df = extract_csv("data/raw/people.csv")
        relations_df = extract_csv("data/raw/relationships.csv")

        # 2. Extract Database
        db_name = str(DB_STAGING)
        tables = extract_table_name(db_name)
        dataframes = {
            table: extract_databse(db_name, table) for table in tables  # type: ignore
        }

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


if __name__ == "__main__":
    run_pipeline()
