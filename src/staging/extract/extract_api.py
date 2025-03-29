import math
from typing import Any, Dict
from tenacity import retry, stop_after_attempt
from pyspark import Row  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from src.utils.api_conn import APIConnector
from src.utils.spark_session import init_spark_session
from src.utils.log_message import log_operation


@retry(stop=stop_after_attempt(3))
def extract_api(start_date: str, end_date: str) -> DataFrame:
    """
    Extracts data from an API within a specified date range and returns it as a Spark DataFrame.

    This function initializes a Spark session, retrieves data from an API using the APIConnector,
    cleans NaN values, and logs the operation status. If the API returns no data or an error occurs,
    an empty DataFrame with a minimal schema is returned. The function retries the extraction up to
    three times in case of failure.

    Args:
        start_date (str): The start date for data extraction.
        end_date (str): The end date for data extraction.

    Returns:
        DataFrame | None: A Spark DataFrame containing the extracted data, or None if extraction fails.
    """
    spark: SparkSession = init_spark_session()

    try:
        api = APIConnector()

        # Get raw data from API
        raw_data: Dict[str, Any] = api.fetch_data(start_date, end_date)

        if not raw_data:
            log_operation(
                step="extract",
                process="staging",
                status="skipped",
                source="api",
                table_name="milestones",
                message="API mengembalikan data kosong",
            )
            # Return empty DataFrame with minimal schema
            return spark.createDataFrame([], ["milestone_id", "object_id"])  # type: ignore

        # Clean NaN values in the raw data
        cleaned_data = [
            Row(
                **{
                    k: (None if isinstance(v, float) and math.isnan(v) else v)
                    for k, v in record.items()  # type: ignore
                }
            )
            for record in raw_data
        ]

        # If no records could be processed, return empty DataFrame
        if not cleaned_data:
            log_operation(
                step="extract",
                process="staging",
                status="skipped",
                source="api",
                table_name="milestones",
                message="Tidak ada data valid setelah cleaning",
            )
            return spark.createDataFrame([], ["milestone_id", "object_id"])  # type: ignore

        # Let Spark infer the schema based on the data
        df: DataFrame = spark.createDataFrame(rows)  # type: ignore

        # Log the schema that was inferred
        log_operation(
            step="extract",
            process="staging",
            status="success",
            source="api",
            table_name="milestones",
            message=f"Berhasil ekstrak {df.count()} records",
        )

        return df

    except Exception as e:
        log_operation(
            step="extract",
            process="staging",
            status="failed",
            source="api",
            table_name="milestones",
            error_msg=str(e),
        )
        return spark.createDataFrame([], ["milestone_id", "object_id"])  # type: ignore


def isEmpty(df: DataFrame) -> Any:
    return df.count() == 0
