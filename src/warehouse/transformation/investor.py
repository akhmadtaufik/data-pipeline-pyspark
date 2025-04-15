from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from src.utils.log_message import log_operation


def transform_investor(df: DataFrame) -> DataFrame | None:
    """
    Transforms the investor DataFrame by extracting distinct investor object IDs and adding
    metadata columns.

    This function processes the input DataFrame to create a new DataFrame with distinct
    investor object IDs and additional columns for metadata such as timestamps and status
    flags. It logs the operation status using the `log_operation` function.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing investor data to be transformed.

    Returns
    -------
    DataFrame or None
        A transformed DataFrame with additional metadata columns, or None if an error occurs.

    Raises
    ------
    Exception
        If the transformation process fails, an exception is raised after logging the error.
    """
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
