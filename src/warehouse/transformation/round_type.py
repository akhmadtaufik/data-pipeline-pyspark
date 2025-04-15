from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
from src.utils.log_message import log_operation


def transform_round_type(df: DataFrame) -> DataFrame | None:
    try:
        # Extract distinct round types from the funding_rounds table
        transform_df = (
            df.select(  # type: ignore
                col("funding_round_type"), col("funding_round_code")
            )
            .distinct()
            .withColumn("description", lit(None).cast("string"))
            .withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
        )

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="funding_rounds",
            message="Successfully transformed funding round data for dim_round_type",
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
            message="Failed to transform funding round data for dim_round_type",
        )

        raise
