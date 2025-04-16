from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
from src.utils.log_message import log_operation


def transform_location(df: DataFrame) -> DataFrame | None:
    """
    Transforms the input DataFrame by selecting and renaming specific columns for the
    dim_location table, adding metadata columns, and logging the operation.

    This function processes the input DataFrame to create a transformed DataFrame with
    renamed columns and additional metadata columns such as 'valid_from', 'valid_to',
    'is_current', 'created_at', and 'updated_at'. It logs the success or failure of the
    transformation operation using the log_operation function.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing location data to be transformed.

    Returns
    -------
    DataFrame or None
        The transformed DataFrame with selected and renamed columns, or None if an
        exception occurs during transformation.

    Raises
    ------
    Exception
        If an error occurs during the transformation process, the exception is logged
        and re-raised.
    """
    try:
        # Select and rename columns according to the mapping
        transform_df = df.select(  # type: ignore
            col("office_id").alias("office_id_nk"),
            col("region"),
            col("address1"),
            col("address2"),
            col("city"),
            col("zip_code"),
            col("state_code"),
            col("country_code"),
            col("latitude"),
            col("longitude"),
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
            table_name="location",
            message="Successfully transformed company data for dim_location",
        )

        return transform_df

    except Exception as e:
        log_operation(
            step="transform",
            process="warehouse",
            status="failed",
            source="staging DataFrame",
            table_name="location",
            error_msg=str(e),
            message="Failed to transform company data for dim_location",
        )
        raise
