from datetime import datetime
from minio import Minio
from io import BytesIO
from pyspark.sql import DataFrame
from urllib.parse import urlparse
from src.utils.log_message import log_operation
from src.utils.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET_NAME,
)


def handle_warehouse_error(
    df: DataFrame, target_table: str, error: Exception, source: str
) -> None:
    """
    Handles errors during warehouse operations by logging the error and backing up the DataFrame to MinIO.

    This function checks if the provided DataFrame is valid and logs an operation status. If valid,
    it attempts to back up the DataFrame as a CSV file to a MinIO bucket. If the bucket does not exist,
    it creates one. The function logs the success or failure of the backup operation.

    Parameters:
    - df (DataFrame): The DataFrame to be backed up.
    - target_table (str): The name of the target table associated with the DataFrame.
    - error (Exception): The exception that triggered the error handling.
    - source (str): The source of the data being processed.

    Returns:
    - None
    """
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    try:
        if df is None or not isinstance(df, DataFrame):
            log_operation(
                step="backup",
                process="warehouse",
                status="failed",
                source=source,
                table_name=target_table,
                error_msg="DataFrame tidak valid untuk backup",
            )
            return

        parsed_endpoint = urlparse(str(MINIO_ENDPOINT))
        endpoint = parsed_endpoint.netloc or parsed_endpoint.path
        client = Minio(
            endpoint=endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=(parsed_endpoint.scheme == "https"),
        )

        if not client.bucket_exists(str(MINIO_BUCKET_NAME)):
            client.make_bucket(str(MINIO_BUCKET_NAME))

        csv_bytes = df.toPandas().to_csv(index=False).encode("utf-8")  # type: ignore
        csv_buffer = BytesIO(csv_bytes)  # type: ignore
        file_name = f"warehouse_{target_table}_error_{current_date}.csv"

        client.put_object(
            bucket_name=str(MINIO_BUCKET_NAME),
            object_name=file_name,
            data=csv_buffer,
            length=len(csv_bytes),  # type: ignore
            content_type="text/csv",
        )

        log_operation(
            step="backup",
            process="warehouse",
            status="success",
            source=source,
            table_name=target_table,
            message=f"Data gagal di-backup ke MinIO: {file_name}",
        )

    except Exception as e:
        log_operation(
            step="backup",
            process="warehouse",
            status="failed",
            source=source,
            table_name=target_table,
            error_msg=f"Gagal backup: {str(e)}",
        )
