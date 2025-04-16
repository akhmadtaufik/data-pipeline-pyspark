from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
from src.utils.log_message import log_operation


def transform_relationship_type(df: DataFrame) -> DataFrame | None:
    """
    Transform relationship data from staging to fit warehouse dimension schema.

    This function takes a DataFrame representing the staging.relationships table
    and transforms it to extract distinct relationship types for the
    warehouse.dim_relationship_type schema.

    Parameters:
    - df (DataFrame): Source DataFrame from staging.relationships

    Returns:
    - DataFrame: Transformed DataFrame ready for warehouse.dim_relationship_type
    """
    try:
        # Extract distinct titles from the relationships table, filtering out null titles
        transform_df = (
            df.select(col("title"), col("created_at"))  # type: ignore
            .distinct()
            .filter(col("title").isNotNull())
            .withColumn("description", lit(None).cast("string"))
            .withColumn("updated_at", current_timestamp())
        )

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="relationships",
            message="Successfully transformed relationship data for dim_relationship_type",
        )

        return transform_df

    except Exception as e:
        log_operation(
            step="transform",
            process="warehouse",
            status="failed",
            source="staging DataFrame",
            table_name="relationships",
            error_msg=str(e),
            message="Failed to transform relationship data for dim_relationship_type",
        )

        raise
