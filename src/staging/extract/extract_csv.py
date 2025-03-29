from pyspark.sql import DataFrame
from src.utils.spark_session import init_spark_session
from src.utils.log_message import log_operation


def extract_csv(file_path: str):
    """
    Extracts data from a CSV file into a DataFrame and logs the operation.

    This function initializes a SparkSession, reads a CSV file into a DataFrame,
    and logs the success or failure of the extraction process. The log includes
    details such as the step, status, source, and table name.

    Args:
        file_path (str): The path to the CSV file to be extracted.

    Returns:
        DataFrame | None: The DataFrame containing the extracted data if successful,
        otherwise None if an error occurs during extraction.
    """
    spark = init_spark_session()
    table_name = file_path.split("/")[-1].split(".")[0]

    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        log_operation(
            step="extract",
            process="staging",
            status="success",
            source="csv",
            table_name=table_name,
            message=f"Berhasil ekstrak {table_name}",
        )

        return df

    except Exception as e:
        log_operation(
            step="extract",
            process="staging",
            status="failed",
            source="csv",
            table_name=table_name,
            error_msg=str(e),
        )
