from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from src.utils.log_message import log_operation


def transform_investor(df: DataFrame) -> DataFrame | None:
    try:
        # Extract distinct investor object IDs
        transform_df = (
            df.select(col("investor_object_id").alias("object_id_nk"))  # type: ignore
            .distinct()
            .withColumn("valid_from", current_timestamp())
            .withColumn("valid_to", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
            .withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
        )

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="investments",
            message="Successfully transformed investments data for dim_investor",
        )

        return transform_df

    except Exception as e:
        log_operation(
            step="transform",
            process="warehouse",
            status="failed",
            source="staging DataFrame",
            table_name="investments",
            error_msg=str(e),
            message="Failed to transform investments data for dim_investor",
        )
        raise
