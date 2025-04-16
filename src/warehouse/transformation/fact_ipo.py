from pyspark.sql import DataFrame, SparkSession
from src.utils.log_message import log_operation
from src.utils.spark_connection import read_jdbc
from src.utils.config import DB_WAREHOUSE


def transform_ipo(df: DataFrame, spark: SparkSession) -> DataFrame | None:
    try:
        # Load company dimension for lookup
        dim_company_df = read_jdbc(
            db=str(DB_WAREHOUSE), schema="warehouse", table_name="dim_company"
        )

        # Register DataFrames as temp views
        dim_company_df.createOrReplaceTempView("dim_company")
        df.createOrReplaceTempView("ipos")

        transform_df = spark.sql(  # type: ignore
            """
            SELECT
                i.ipo_id AS ipo_id_nk,
                CAST(dc.company_id AS STRING) AS company_id,
                CAST(date_format(i.public_at, 'yyyyMMdd') AS INT) AS ipo_date_id,
                i.valuation_amount,
                i.valuation_currency_code,
                i.raised_amount,
                i.raised_currency_code,
                i.stock_symbol,
                i.source_url,
                i.source_description,
                i.created_at,
                current_timestamp() AS updated_at
            FROM ipos i
            LEFT JOIN dim_company dc ON i.object_id = dc.object_id_nk
            """
        )

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="ipos",
            message="Successfully transformed funding round data for ipos",
        )

        return transform_df

    except Exception as e:
        log_operation(
            step="transform",
            process="warehouse",
            status="failed",
            source="staging DataFrame",
            table_name="ipos",
            error_msg=str(e),
            message="Failed to transform ipo data for fact_ipo",
        )
        raise
