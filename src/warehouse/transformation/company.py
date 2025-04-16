from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
from src.utils.log_message import log_operation


def transform_company(df: DataFrame) -> DataFrame | None:
    """
    Transform company data from staging to fit warehouse dimension schema.

    This function takes a DataFrame representing the staging.company table
    and transforms it to match the warehouse.dim_company schema requirements.

    Parameters:
    - df (DataFrame): Source DataFrame from staging.company

    Returns:
    - DataFrame: Transformed DataFrame ready for warehouse.dim_company
    """
    try:
        # Select and rename columns according to the mapping
        transform_df: DataFrame = df.select(  # type: ignore
            col("object_id").alias("object_id_nk"),
            col("description"),
            col("region"),
            col("city"),
            col("state_code"),
            col("country_code"),
            current_timestamp().alias("valid_from"),
            lit(None).cast("timestamp").alias("valid_to"),
            lit(True).alias("is_current"),
            col("created_at"),
            current_timestamp().alias("updated_at"),
        )

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="company",
            message="Successfully transformed company data for dim_company",
        )

        return transform_df

    except Exception as e:
        log_operation(
            step="transform",
            process="warehouse",
            status="failed",
            source="staging DataFrame",
            table_name="company",
            error_msg=str(e),
            message="Failed to transform company data for dim_company",
        )
        raise
