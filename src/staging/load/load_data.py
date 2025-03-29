from datetime import datetime
from pyspark.sql import DataFrame
from src.staging.load.handle_error import handle_error
from src.utils.log_message import log_operation
from src.utils.spark_session import init_spark_session
from src.utils.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    DB_STAGING,
)

spark = init_spark_session()


def get_last_etl_date(process_name: str, table_name: str) -> datetime:
    return logger.get_last_run(process_name, table_name)  # type: ignore


def load_data_to_staging(
    df: DataFrame, target_table: str, source: str, schema: str = "staging"
) -> None:
    """
    Loads data from a DataFrame into a staging table in a PostgreSQL database.

    This function filters the DataFrame for new data based on the last ETL date,
    writes the filtered data to a temporary table, and performs an upsert operation
    into the target staging table. It logs the operation status and handles any
    errors that occur during the process.

    Args:
        df (DataFrame): The DataFrame containing the data to be loaded.
        target_table (str): The name of the target table in the staging schema.
        source (str): The source of the data being loaded.
        schema (str, optional): The schema where the target table resides. Defaults to "staging".

    Raises:
        Exception: If an error occurs during the data loading process.
    """
    full_table_name = f"{schema}.{target_table}"
    temp_table_name = f"temp_{target_table}"

    try:
        log_operation(
            step="load",
            process="staging",
            status="started",
            source=source,
            table_name=target_table,
            message="Memulai proses load",
        )

        # Get and filter data using SQL query syntax
        last_etl_date: datetime = get_last_etl_date(schema, target_table)
        date_filter: str = last_etl_date.strftime("%Y-%m-%d %H:%M:%S")

        if "etl_date" in df.columns:
            new_data: DataFrame = df.filter(f"etl_date > '{date_filter}'")  # type: ignore
        else:
            new_data = df

        # Write to temporary table
        new_data.write.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_STAGING}",
            table=temp_table_name,
            mode="overwrite",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )

        # Execute upsert
        spark.sql(  # type: ignore
            f"""
            INSERT INTO {full_table_name}
            SELECT * FROM {temp_table_name}
            ON CONFLICT (id)
            DO UPDATE SET
                updated_at = EXCLUDED.updated_at,
                etl_date = EXCLUDED.etl_date
        """
        )

        log_operation(
            step="load",
            process="staging",
            status="success",
            source=source,
            table_name=target_table,
            message="Data berhasil di-load",
        )

    except Exception as e:
        handle_error(new_data, target_table, e, source)

        log_operation(
            step="load",
            process="staging",
            status="failed",
            source=source,
            table_name=target_table,
            error_msg=str(e),
        )

        raise

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")  # type: ignore
