from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
from src.utils.log_message import log_operation


def transform_fund(df: DataFrame) -> DataFrame | None:
    """
    Transform funds data from staging to fit warehouse dimension schema.

    This function takes a DataFrame representing the staging.funds table
    and transforms it to match the warehouse.dim_fund schema requirements.

    Parameters:
    - df (DataFrame): Source DataFrame from staging.funds

    Returns:
    - DataFrame: Transformed DataFrame ready for warehouse.dim_fund
    """
    try:
        # Select and rename columns according to the mapping
        transform_df = df.select(  # type: ignore
            col("fund_id").alias("fund_id_nk"),
            col("object_id").alias("object_id_nk"),
            col("name"),
            col("source_url"),
            col("source_description"),
            current_timestamp().alias("valid_from"),
            lit(None).cast("timestamp").alias("valid_to"),
            lit(True).alias("is_current"),
            col("created_at"),
            current_timestamp().alias("updated_at"),
        )

        # Drop duplicate rows based on object_id_nk
        dedup_df = transform_df.dropDuplicates(["object_id_nk"])

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="funds",
            message="Successfully transformed funds data for dim_fund",
        )

        return dedup_df

    except Exception as e:
        log_operation(
            step="transform",
            process="warehouse",
            status="failed",
            source="staging DataFrame",
            table_name="funds",
            error_msg=str(e),
            message="Failed to transform funds data for dim_fund",
        )
        raise
