# type: ignore
import math
from tenacity import retry, stop_after_attempt
from src.utils.api_conn import APIConnector
from src.utils.spark_session import init_spark_session
from pyspark import Row
from pyspark.sql import DataFrame

@retry(stop=stop_after_attempt(3))
def extract_api(start_date: str, end_date: str) -> DataFrame:
    """
    Extracts milestone data from API and returns it as a Spark DataFrame.
    Uses a dynamic schema to capture all fields returned by the API.
    """
    spark = init_spark_session()

    try:
        api = APIConnector()

        # Get raw data from API
        raw_data = api.fetch_data(start_date, end_date)

        if not raw_data:
            print("API mengembalikan data kosong.")
            # Return empty DataFrame with minimal schema
            return spark.createDataFrame([], ["milestone_id", "object_id"])

        # Clean NaN values in the raw data
        cleaned_data = []
        for record in raw_data:
            cleaned_record = {}
            for key, value in record.items():
                if isinstance(value, float) and math.isnan(value):
                    cleaned_record[key] = None
                else:
                    cleaned_record[key] = value
            cleaned_data.append(cleaned_record)

        # If no records could be processed, return empty DataFrame
        if not cleaned_data:
            print("Tidak ada data valid setelah dibersihkan.")
            return spark.createDataFrame([], ["milestone_id", "object_id"])

        # Convert dictionary to Row objects for dynamic schema inference
        rows = [Row(**record) for record in cleaned_data]

        # Let Spark infer the schema based on the data
        df = spark.createDataFrame(rows)

        # Log the schema that was inferred
        print("Schema terdeteksi dari API:")
        df.printSchema()

        return df

    except Exception as e:
        error_message = str(e)
        print(f"Error saat menghubungi API: {error_message}")
        # Return empty DataFrame with minimal fields
        return spark.createDataFrame([], ["milestone_id", "object_id"])

def isEmpty(df):
    """Helper method to check if DataFrame is empty"""
    return df.count() == 0
