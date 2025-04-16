from pyspark.sql import DataFrame, SparkSession
from src.utils.log_message import log_operation
from src.utils.spark_connection import read_jdbc
from src.utils.config import DB_WAREHOUSE


def transform_acquisition(
    df: DataFrame, spark: SparkSession
) -> DataFrame | None:
    try:
        # Load company dimension for lookup
        dim_company_df = read_jdbc(
            db=str(DB_WAREHOUSE), schema="warehouse", table_name="dim_company"
        )

        # Register DataFrame
        dim_company_df.createOrReplaceTempView("dim_company")
        df.createOrReplaceTempView("acquisitions")

        transform_df = spark.sql(  # type: ignore
            """
            SELECT
                a.acquisition_id AS acquisition_id_nk,
                CAST(acq.company_id AS STRING) AS acquiring_company_id,
                CAST(acquired.company_id AS STRING) AS acquired_company_id,
                CAST(date_format(a.acquired_at, 'yyyyMMdd') AS INT) AS acquisition_date_id,
                a.term_code,
                a.price_amount,
                a.price_currency_code,
                a.source_url,
                a.source_description,
                current_timestamp() AS created_at,
                current_timestamp() AS updated_at
            FROM acquisitions a
            LEFT JOIN dim_company acq ON a.acquiring_object_id = acq.object_id_nk
            LEFT JOIN dim_company acquired ON a.acquired_object_id = acquired.object_id_nk
            """
        )

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="acquisition",
            message="Successfully transformed acquisition data for fact_acquisition",
        )

        return transform_df

    except Exception as e:
        log_operation(
            step="transform",
            process="warehouse",
            status="failed",
            source="staging DataFrame",
            table_name="acquisition",
            error_msg=str(e),
            message="Failed to transform acquisition data for fact_acquisition",
        )
        raise
