import gc
from typing import Dict, List
from pyspark.sql import DataFrame
from src.utils.log_message import log_operation
from src.utils.spark_session import init_spark_session
from src.warehouse.extract.extract_staging import extract_staging
from src.warehouse.transformation.company import transform_company
from src.warehouse.transformation.location import transform_location
from src.warehouse.transformation.person import transform_person
from src.warehouse.transformation.fund import transform_fund
from src.warehouse.transformation.round_type import transform_round_type
from src.warehouse.transformation.relationship_type import (
    transform_relationship_type,
)
from src.warehouse.transformation.investor import transform_investor
from src.warehouse.transformation.fact_funding_round import (
    transform_funding_round,
)
from src.warehouse.transformation.fact_ipo import transform_ipo
from src.warehouse.transformation.fact_acquisition import transform_acquisition
from src.warehouse.load.load_warehouse import load_warehouse


def run_warehouse_pipeline() -> None:
    try:
        spark = init_spark_session()

        # Store DataFrames
        staging_dataframes: Dict[str, DataFrame] = {}
        dimension_dataframes: Dict[str, DataFrame] = {}
        fact_dataframes: Dict[str, DataFrame] = {}

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

        # Transform person dimension
        if "people" in staging_dataframes:
            dim_person_df = transform_person(staging_dataframes["people"])
            dimension_dataframes["dim_person"] = dim_person_df  # type: ignore
            print(f"Successfully stored dim_person DataFrame")

        # Transform fund dimension
        if "funds" in staging_dataframes:
            dim_fund_df = transform_fund(staging_dataframes["funds"])
            dimension_dataframes["dim_fund"] = dim_fund_df  # type: ignore
            print(f"Successfully stored dim_fund DataFrame")

        # Transform relationship type dimension
        if "relationships" in staging_dataframes:
            dim_relationship_type_df = transform_relationship_type(
                staging_dataframes["relationships"]
            )
            dimension_dataframes["dim_relationship_type"] = dim_relationship_type_df  # type: ignore
            print(f"Successfully stored dim_relationship_type DataFrame")

        # Transform round type dimension
        if "funding_rounds" in staging_dataframes:
            dim_round_type_df = transform_round_type(
                staging_dataframes["funding_rounds"]
            )
            dimension_dataframes["dim_round_type"] = dim_round_type_df  # type: ignore
            print(f"Successfully stored dim_round_type DataFrame")

        # Transform investor dimension
        if "investments" in staging_dataframes:
            dim_investor_df = transform_investor(
                staging_dataframes["investments"]
            )
            dimension_dataframes["dim_investor"] = dim_investor_df  # type: ignore
            print(f"Successfully stored dim_investor DataFrame")

        # Step 3: Load transformed data into warehouse (independent ones)
        log_operation(
            step="load",
            process="warehouse",
            status="info",
            source="dimension dataframes",
            table_name="system",
            message="Starting loading data into warehouse",
        )

        # Load dimension tables first
        dimension_load_order: List[str] = [
            "dim_company",
            "dim_location",
            "dim_person",
            "dim_fund",
            "dim_relationship_type",
            "dim_round_type",
            "dim_investor",
        ]

        # Step 3.1: Load dimension tables
        for dim_name in dimension_load_order:
            if dim_name in dimension_dataframes:
                load_warehouse(
                    df=dimension_dataframes[dim_name],
                    table_name=dim_name,
                )

                dimension_dataframes[dim_name].unpersist()
                spark.catalog.clearCache()
                gc.collect()

        # Step 4: Process fact tables based on dimensions
        # Transform funding round fact
        if "funding_rounds" in staging_dataframes:
            fact_funding_round_df = transform_funding_round(
                staging_dataframes["funding_rounds"], spark
            )
            fact_dataframes["fact_funding_round"] = fact_funding_round_df  # type: ignore
            print("Successfully stored fact_funding_round DataFrame")

        # Transform IPO fact
        if "ipos" in staging_dataframes:
            fact_ipo_df = transform_ipo(staging_dataframes["ipos"], spark)
            fact_dataframes["fact_ipo"] = fact_ipo_df  # type: ignore
            print("Successfully stored fact_ipo DataFrame")

        # Transform acquisition fact
        if "acquisition" in staging_dataframes:
            fact_acquisition_df = transform_acquisition(
                staging_dataframes["acquisition"], spark
            )
            fact_dataframes["fact_acquisition"] = fact_acquisition_df  # type: ignore
            print("Successfully stored fact_ipo DataFrame")

        # Step 4.1: Load fact tables
        fact_load_order: List[str] = [
            "fact_funding_round",
            "fact_ipo",
            "fact_acquisition",
        ]

        for fact_name in fact_load_order:
            if fact_name in fact_dataframes:
                if fact_name == "fact_funding_round":
                    # Gunakan mode 'upsert' untuk menghindari full load
                    load_warehouse(
                        df=fact_dataframes[fact_name],
                        table_name=fact_name,
                        mode="upsert"
                    )
                else:
                    load_warehouse(
                        df=fact_dataframes[fact_name], table_name=fact_name
                    )

                fact_dataframes[fact_name].unpersist()
                spark.catalog.clearCache()
                gc.collect()

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
