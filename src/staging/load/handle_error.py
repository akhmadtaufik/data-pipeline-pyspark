from datetime import datetime
from pyspark.sql import DataFrame
from minio import Minio
from io import BytesIO
from urllib.parse import urlparse
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
    Handles errors by attempting to back up a DataFrame to a MinIO bucket and logs the operation.

    This function checks if the provided DataFrame is valid and attempts to back it up as a CSV file
    to a specified MinIO bucket. It logs the success or failure of each operation step using the
    `log_operation` function. If the DataFrame is None or not of the correct type, it logs an error
    and exits. If the backup process encounters any errors, it logs the error details.

    Parameters:
    - df (DataFrame): The DataFrame to be backed up.
    - target_table (str): The name of the target table for the backup.
    - error (Exception): The exception that triggered the error handling.
    - source (str): The source of the data being processed.

    Returns:
    - None
    """
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    try:
        if df is None:
            log_operation(
                step="backup",
                process="staging",
                status="failed",
                source=source,
                table_name=target_table,
                error_msg="Cannot backup: DataFrame is None",
            )
            return

        if not isinstance(df, DataFrame):
            log_operation(
                step="backup",
                process="staging",
                status="failed",
                source=source,
                table_name=target_table,
                error_msg="Invalid DataFrame type",
            )

        parsed_endpoint = urlparse(str(MINIO_ENDPOINT))

        endpoint = (
            parsed_endpoint.netloc
            if parsed_endpoint.netloc
            else parsed_endpoint.path
        )

        # Initialize MinIO client
        client = Minio(
            endpoint=endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=(parsed_endpoint.scheme == "https"),
        )

        # Make a bucket if it doesn't exist
        if not client.bucket_exists(str(MINIO_BUCKET_NAME)):
            client.make_bucket(str(MINIO_BUCKET_NAME))

        # Convert DataFrame to CSV and then to bytes
        try:
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
        except Exception as csv_error:
            # Handle specific errors when converting to CSV
            log_operation(
                step="backup",
                process="staging",
                status="failed",
                source=source,
                table_name=target_table,
                error_msg=f"Gagal mengkonversi DataFrame ke CSV: {str(csv_error)}",
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
