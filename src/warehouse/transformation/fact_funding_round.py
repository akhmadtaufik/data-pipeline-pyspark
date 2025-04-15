from pyspark.sql import DataFrame, SparkSession
from src.utils.spark_session import init_spark_session
from src.utils.log_message import log_operation
from src.utils.spark_connection import read_jdbc
from src.utils.config import DB_WAREHOUSE


def transform_funding_round(
    df: DataFrame, spark: SparkSession
) -> DataFrame | None:
    """
    Transforms funding round data by performing lookups and joins with dimension tables.

    This function initializes a SparkSession, loads dimension tables from a database,
    and registers them as temporary views. It then performs SQL transformations on the
    input DataFrame by joining it with the dimension tables to enrich the data. The
    transformed DataFrame is returned with specific columns cast to string type.

    Args:
        df (DataFrame): The input DataFrame containing funding round data.
        spark (SparkSession): The SparkSession instance.

    Returns:
        DataFrame | None: The transformed DataFrame with enriched funding round data,
        or None if an error occurs during the transformation process.

    Raises:
        Exception: If an error occurs during the transformation, it logs the error
        and re-raises the exception.
    """
    spark = init_spark_session()
    try:
        # Load dimension tables for lookups
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

        # Register DataFrames as temp views for SQL operations
        dim_company_df.createOrReplaceTempView("dim_company")
        dim_round_type_df.createOrReplaceTempView("dim_round_type")
        df.createOrReplaceTempView("funding_rounds")

        # Transform funding round data with lookups to dimension tables
        transform_df = spark.sql(  # type: ignore
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

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="funding_rounds",
            message="Successfully transformed funding round data for fact_funding_round",
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
