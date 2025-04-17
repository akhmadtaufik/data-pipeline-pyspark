from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from src.utils.spark_session import init_spark_session
from src.utils.log_message import log_operation
from src.utils.spark_connection import read_jdbc
from src.utils.config import DB_WAREHOUSE


def transform_funding_round(
    df: DataFrame, spark: SparkSession, batch_size: int = 5000
) -> DataFrame | None:
    """
    Transforms funding round data incrementally by processing data in small batches.

    This function initializes a SparkSession, loads dimension tables from a database,
    and registers them as temporary views. It then performs SQL transformations on the
    input DataFrame by joining it with the dimension tables to enrich the data. The
    transformation is done in batches to optimize memory usage and processing efficiency.

    Args:
        df (DataFrame): The input DataFrame containing funding round data.
        spark (SparkSession): The SparkSession instance.
        batch_size (int, optional): Size of each batch for incremental processing. Defaults to 5000.

    Returns:
        DataFrame | None: The transformed DataFrame with enriched funding round data,
        or None if an error occurs during the transformation process.

    Raises:
        Exception: If an error occurs during the transformation, it logs the error
        and re-raises the exception.
    """
    spark = init_spark_session()
    try:
        # Calculate total number of records to process
        total_records = df.count()
        log_operation(
            step="transform",
            process="warehouse",
            status="info",
            source="staging DataFrame",
            table_name="funding_rounds",
            message=f"Starting incremental processing of {total_records} funding round records",
        )

        # Load dimension tables for lookups (only once)
        dim_company_df = read_jdbc(
            db=str(DB_WAREHOUSE),
            schema="warehouse",
            table_name="dim_company",
        )

        dim_round_type_df = read_jdbc(
            db=str(DB_WAREHOUSE),
            schema="warehouse",
            table_name="dim_round_type",
        )

        # Register dimension tables as temp views (only once)
        dim_company_df.createOrReplaceTempView("dim_company")
        dim_round_type_df.createOrReplaceTempView("dim_round_type")

        # Add row number for batching
        df = df.coalesce(2)
        window_spec = Window.orderBy("funding_round_id")  # type: ignore
        df = df.withColumn("row_id", row_number().over(window_spec))

        # Initialize an empty list to store transformed batches
        transformed_batches: List[DataFrame] = []

        # Process data in batches
        for batch_start in range(0, total_records, batch_size):
            batch_end = batch_start + batch_size

            log_operation(
                step="transform",
                process="warehouse",
                status="info",
                source="staging DataFrame",
                table_name="funding_rounds",
                message=f"Processing batch {batch_start+1} to {min(batch_end, total_records)} of {total_records}",
            )

            # Extract current batch
            current_batch = df.filter(  # type: ignore
                (df.row_id >= batch_start + 1) & (df.row_id <= batch_end)  # type: ignore
            ).drop("row_id")

            # Skip processing if batch is empty
            if current_batch.rdd.isEmpty():
                continue

            # Register current batch as temp view
            current_batch.createOrReplaceTempView("funding_rounds")

            # Transform current batch with lookups to dimension tables
            batch_transform_df = spark.sql(  # type: ignore
                """
                SELECT
                    fr.funding_round_id AS funding_round_id_nk,
                    CAST(dc.company_id AS STRING) AS company_id,
                    CAST(drt.round_type_id AS STRING) AS round_type_id,
                    CAST(DATE_FORMAT(fr.funded_at, 'yyyyMMdd') AS INT) AS funded_date_id,
                    fr.raised_amount_usd,
                    fr.raised_amount,
                    fr.raised_currency_code,
                    fr.pre_money_valuation_usd,
                    fr.pre_money_valuation,
                    fr.pre_money_currency_code,
                    fr.post_money_valuation_usd,
                    fr.post_money_valuation,
                    fr.post_money_currency_code,
                    fr.participants,
                    fr.is_first_round,
                    fr.is_last_round,
                    fr.source_url,
                    fr.source_description
                FROM funding_rounds fr
                LEFT JOIN dim_company dc ON fr.object_id = dc.object_id_nk
                LEFT JOIN dim_round_type drt ON
                    fr.funding_round_type = drt.funding_round_type AND
                    COALESCE(fr.funding_round_code, '') = COALESCE(drt.funding_round_code, '')
                """
            )

            # Add transformed batch to the list
            transformed_batches.append(batch_transform_df)

            # Force checkpoint to release memory periodically
            if len(transformed_batches) % 5 == 0:
                spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoint")
                batch_transform_df.checkpoint()

        # Union all transformed batches
        if not transformed_batches:
            log_operation(
                step="transform",
                process="warehouse",
                status="warning",
                source="staging DataFrame",
                table_name="funding_rounds",
                message="No data to process in funding round transformation",
            )
            return None

        transform_df = transformed_batches[0]
        for batch_df in transformed_batches[1:]:
            transform_df = transform_df.unionAll(batch_df)

        # Drop duplicate rows based on funding_round_id_nk
        transform_df = transform_df.dropDuplicates(["funding_round_id_nk"])

        # Cache the final result to improve performance
        transform_df = transform_df.cache()

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="funding_rounds",
            message=f"Successfully transformed {transform_df.count()} funding round records incrementally",
        )

        return transform_df

    except Exception as e:
        log_operation(
            step="transform",
            process="warehouse",
            status="failed",
            source="staging DataFrame",
            table_name="funding_rounds",
            error_msg=str(e),
            message="Failed to transform funding round data for fact_funding_round",
        )
        raise
