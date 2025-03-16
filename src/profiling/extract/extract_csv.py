from pyspark.sql import DataFrame
from src.utils.spark_session import init_spark_session


def extract_csv(file_path: str) -> DataFrame:
    """
    Extract a CSV file into a DataFrame using a SparkSession.

    This function initializes a SparkSession and reads a CSV file from the
    specified file path into a DataFrame. The CSV file is expected to have a
    header row, and the schema is inferred automatically.

    Args:
        file_path (str): The path to the CSV file to be read.

    Returns:
        DataFrame: A DataFrame containing the data from the CSV file.
    """
    spark = init_spark_session()

    return spark.read.csv(file_path, header=True, inferSchema=True)
