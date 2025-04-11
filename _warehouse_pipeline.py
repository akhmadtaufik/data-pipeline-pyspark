from typing import Dict, List
from pyspark.sql import DataFrame
from src.utils.log_message import log_operation
from src.utils.spark_session import init_spark_session
from src.warehouse.extract.extract_staging import extract_staging
from src.warehouse.transformation.company import transform_company
from src.warehouse.transformation.location import transform_location


def run_warehouse_pipeline() -> None:
    try:
        spark = init_spark_session()

        # Store DataFrames
        staging_dataframes: Dict[str, DataFrame] = {}
        dimension_dataframes: Dict[str, DataFrame] = {}

        # Step 1: Extract all required tables
        log_operation(
            step="extract",
            process="warehouse",
            status="info",
            source="staging area",
            table_name="system",
            message="Starting extraction from staging area",
        )

        staging_tables: List[str] = [
            "company",
            "people",
            "funds",
            "relationships",
            "funding_rounds",
            "investments",
            "milestones",
            "acquisition",
            "ipos",
        ]

        for table_name in staging_tables:
            df = extract_staging(table_name)

            if df is not None:
                staging_dataframes[table_name] = df

        # Step 2: Process dimension tables based on extracted data
        log_operation(
            step="transform",
            process="warehouse",
            status="info",
            source="staging DataFrame",
            table_name="system",
            message="Starting transformation for dimension tables",
        )
        # Transform company dimension
        if "company" in staging_dataframes:
            dim_company_df = transform_company(staging_dataframes["company"])
            dimension_dataframes["dim_company"] = dim_company_df  # type: ignore
            print(f"Successfully stored dim_company DataFrame")

        # Transform location dimension
        if "company" in staging_dataframes:
            dim_location_df = transform_location(staging_dataframes["company"])
            dimension_dataframes["dim_location"] = dim_location_df  # type: ignore
            print(f"Successfully stored dim_location DataFrame")

        # Step 3: Process fact tables based on dimensions

        # Step 4: Load transformed data into warehouse
        log_operation(
            step="load",
            process="warehouse",
            status="info",
            source="dimension dataframes",
            table_name="system",
            message="Starting loading data into warehouse",
        )

        # TODO: Add loading logic for dimension and fact tables

        log_operation(
            step="pipeline",
            process="warehouse",
            status="success",
            source="system",
            table_name="system",
            message="Warehouse pipeline completed successfully",
        )

    except Exception as e:
        log_operation(
            step="pipeline",
            process="warehouse",
            status="failed",
            source="staging area",
            table_name="",
            error_msg=str(e),
            message="Warehouse pipeline failed with error",
        )


if __name__ == "__main__":
    run_warehouse_pipeline()
