from typing import Any, Dict
from src.profiling.base_profiler import BaseProfiler
from src.utils.spark_session import init_spark_session


class CSVProfiler(BaseProfiler):
    """
    A profiler class for analyzing CSV files using Spark.

    This class extends the BaseProfiler to provide profiling capabilities
    for CSV files. It initializes a SparkSession and reads the CSV file
    specified by the file path. The profile method returns a dictionary
    containing metadata about each column, including data type, number of
    missing values, and count of unique values.

    Args:
        file_path (str): The path to the CSV file to be profiled.
    """

    def __init__(self, file_path: str) -> None:
        self.spark = init_spark_session()
        self.file_path = file_path

    def profile(self) -> Dict[str, Any]:
        df = self.spark.read.csv(self.file_path, header=True, inferSchema=True)

        return {
            "columns": {
                col: {
                    "data_type": str(df.schema[col].dataType),
                    "missing_values": df.filter(df[col].isNull()).count(),
                    "unique_values": df.select(col).distinct().count(),
                }
                for col in df.columns
            }
        }
