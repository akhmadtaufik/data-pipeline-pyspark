import uuid
from pyspark.sql import DataFrame
from src.utils.spark_session import init_spark_session
from src.utils.psycopg2_connection import read_db
from src.utils.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


def read_jdbc(db: str, schema: str, table_name: str) -> DataFrame:
    """
    Reads data from a PostgreSQL database table into a DataFrame using JDBC.

    This function establishes a SparkSession and reads data from the specified
    database, schema, and table using JDBC. It utilizes environment variables
    for database connection details.

    Args:
        db (str): The name of the database.
        schema (str): The schema within the database.
        table_name (str): The name of the table to read.

    Returns:
        DataFrame: A DataFrame containing the data from the specified table.
    """
    spark = init_spark_session()
    df = spark.read.jdbc(
        url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db}",
        table=f"{schema}.{table_name}",
        properties={
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver",
        },  # type: ignore
    )

    return df


def write_jdbc(
    df: DataFrame,
    db: str,
    schema: str,
    table_name: str,
    mode: str = "append",
    upsert_key: str = None,  # type: ignore
) -> None:
    """
    Writes a DataFrame to a PostgreSQL database table using JDBC.

    Parameters:
        df (DataFrame): The DataFrame to be written.
        db (str): The name of the database.
        schema (str): The schema within the database.
        table_name (str): The name of the table to write to.
        mode (str, optional): The write mode, either 'append', 'overwrite', or 'upsert'. Defaults to 'append'.
        upsert_key (str, optional): The column name to use as the key for upsert operations. Required if mode is 'upsert'.

    Raises:
        Exception: If an error occurs during the upsert operation, the transaction is rolled back and the exception is raised.

    Notes:
        - If mode is 'upsert', a temporary table is created for the operation, and an UPSERT is performed using the specified key.
        - The function uses the organization's internal configuration for database connection parameters.
    """
    if mode == "upsert" and upsert_key:
        temp_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"

        # Step 1: Write to temporary table
        df.write.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db}",
            table=f"{schema}.{temp_table}",
            mode="overwrite",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )

        # Step 2: Execute UPSERT
        try:
            conn = read_db(db_name=db)

            cursor = conn.cursor()

            # Generate UPDATE clause
            update_columns = [col for col in df.columns if col != upsert_key]
            set_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )

            query = f"""
                INSERT INTO {schema}.{table_name}
                SELECT * FROM {schema}.{temp_table}
                ON CONFLICT ({upsert_key})
                DO UPDATE SET {set_clause}
            """
            cursor.execute(query)
            conn.commit()

            # Cleanup
            cursor.execute(f"DROP TABLE {schema}.{temp_table}")
            conn.commit()

        except Exception as e:
            conn.rollback()
            raise

        finally:
            cursor.close()
            conn.close()

    else:
        df.write.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db}?stringtype=unspecified",
            table=f"{schema}.{table_name}",
            mode=mode,
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },  # type: ignore
        )
