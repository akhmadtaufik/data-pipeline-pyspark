from typing import Dict, List
from pyspark.sql import DataFrame
from src.utils.log_message import log_operation
from src.warehouse.extract.extract_staging import extract_staging


def run_warehouse_pipeline() -> None:
    try:
        # Store DataFrames
        staging_dataframes: Dict[str, DataFrame] = {}

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
                print(f"Successfully stored {table_name} DataFrame")

            else:
                print(f"Failed to extract {table_name} from staging area")

        # Step 2: Process dimension tables based on extracted data

        # Step 3: Process fact tables based on dimensions

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
