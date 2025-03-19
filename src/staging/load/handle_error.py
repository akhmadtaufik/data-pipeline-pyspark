from datetime import datetime
from pyspark.sql import DataFrame
from minio import Minio
from io import BytesIO
from src.utils.log import ETLLogger
from src.utils.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET_NAME,
)

logger = ETLLogger()


def handle_error(df: DataFrame, target_table: str, error: Exception) -> None:
    """
    Handles errors during the ETL process by backing up the DataFrame to MinIO
    and logging the error details.

    This function attempts to upload the provided DataFrame as a CSV file to a
    specified MinIO bucket. If the bucket does not exist, it is created. The
    function logs the error details using the ETLLogger. If an error occurs
    during the backup process, it logs the failure and raises the exception.

    Args:
        df (DataFrame): The DataFrame to be backed up.
        target_table (str): The name of the target table associated with the DataFrame.
        error (Exception): The exception that triggered the error handling.

    Raises:
        Exception: If an error occurs during the backup process.
    """
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Initialize MinIO client
        client = Minio(
            endpoint=str((MINIO_ENDPOINT)),
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )

        # Make a bucket if it doesn't exist
        if not client.bucket_exists(str(MINIO_BUCKET_NAME)):
            client.make_bucket(str(MINIO_BUCKET_NAME))

        # Convert DataFrame to CSV and then to bytes
        csv_bytes = df.toPandas().to_csv(index=False).encode("utf-8")  # type: ignore
        csv_buffer = BytesIO(csv_bytes)  # type: ignore
        file_name = f"{target_table}_{current_date}.csv"

        # Upload the CSV file to the bucket
        client.put_object(
            bucket_name=str((MINIO_BUCKET_NAME)),
            object_name=file_name,
            data=csv_buffer,
            length=len(csv_bytes),  # type: ignore
            content_type="text/csv",
        )

        # Log error
        logger.log(
            {
                "step": "load",
                "process": "staging",
                "status": "failed",
                "source": "pipeline",
                "table_name": target_table,
                "etl_date": current_date,
            }
        )

    except Exception as e:
        logger.log(
            {
                "step": "load",
                "process": "staging",
                "status": "failed",
                "source": "pipeline",
                "table_name": target_table,
                "etl_date": current_date,
                "error_msg": f"Gagal backup data: {str(e)}",
            }
        )
        raise
