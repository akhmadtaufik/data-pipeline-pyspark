from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from src.utils.spark_session import init_spark_session
from src.utils.log_message import log_operation


def extract_csv(file_path: str) -> DataFrame | None:
    """
    Extracts data from a CSV file into a DataFrame and logs the operation.

    This function initializes a SparkSession, reads a CSV file into a DataFrame,
    and adds timestamp columns for 'created_at' and 'etl_date'. It logs the
    operation's success or failure using the log_operation function.

    Parameters:
        file_path (str): The path to the CSV file to be extracted.

    Returns:
        DataFrame | None: A DataFrame containing the extracted data, or None if
        an error occurs during extraction.
    """
    spark = init_spark_session()
    table_name: str = file_path.split("/")[-1].split(".")[0]

    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Add created_at column in people data
        if table_name == "people":
            df = df.withColumn("created_at", current_timestamp())

        # Add etl_date column
        df = df.withColumn("etl_date", current_timestamp())

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
