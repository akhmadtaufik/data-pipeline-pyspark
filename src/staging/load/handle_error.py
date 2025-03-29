from datetime import datetime
from pyspark.sql import DataFrame
from minio import Minio
from io import BytesIO
from src.utils.log_message import log_operation
from src.utils.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET_NAME,
)


def handle_error(
    df: DataFrame, target_table: str, error: Exception, source: str
) -> None:
    """
    Handles errors by backing up the provided DataFrame to a MinIO bucket and logging the operation.

    Args:
        df (DataFrame): The DataFrame to be backed up.
        target_table (str): The name of the target table associated with the DataFrame.
        error (Exception): The exception that triggered the error handling.
        source (str): The source of the data being processed.

    Raises:
        Exception: Re-raises any exception encountered during the backup process.
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

        log_operation(
            step="backup",
            process="staging",
            status="success",
            source=source,
            table_name=target_table,
            message=f"Backup berhasil: {file_name}",
        )

    except Exception as e:
        log_operation(
            step="backup",
            process="staging",
            status="failed",
            source=source,
            table_name=target_table,
            error_msg=f"Gagal backup: {str(e)}",
        )
        raise
