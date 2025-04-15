from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col,
    concat_ws,
)
from src.utils.log_message import log_operation


def transform_person(df: DataFrame) -> DataFrame | None:
    """
    Transform people data from staging to fit warehouse dimension schema.

    This function takes a DataFrame representing the staging.people table
    and transforms it to match the warehouse.dim_person schema requirements.

    Parameters:
    - df (DataFrame): Source DataFrame from staging.people

    Returns:
    - DataFrame: Transformed DataFrame ready for warehouse.dim_person
    """
    try:
        # Select and rename columns according to the mapping
        transform_df = df.select(  # type: ignore
            col("people_id").alias("people_id_nk"),
            col("object_id").alias("object_id_nk"),
            col("first_name"),
            col("last_name"),
            # Derive full_name field by concatenating first_name and last_name
            concat_ws(" ", col("first_name"), col("last_name")).alias(
                "full_name"
            ),
            col("affiliation_name"),
            current_timestamp().alias("valid_from"),
            lit(None).cast("timestamp").alias("valid_to"),
            lit(True).alias("is_current"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )

        log_operation(
            step="transform",
            process="warehouse",
            status="success",
            source="staging DataFrame",
            table_name="people",
            message="Successfully transformed people data for dim_person",
        )

        return transform_df

    except Exception as e:
        log_operation(
            step="transform",
            process="warehouse",
            status="failed",
            source="staging DataFrame",
            table_name="people",
            error_msg=str(e),
            message="Failed to transform people data for dim_person",
        )

        raise
