import os
from pyspark.sql import DataFrame
from src.utils.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    DB_STAGING,
)


def load_data_to_staging(df: DataFrame, target_table: str):
    """
    Loads a DataFrame into a specified staging table in a PostgreSQL database.

    This function connects to a PostgreSQL database using environment variables
    to retrieve connection details such as host, port, database name, user, and
    password. It writes the provided DataFrame to the specified target table
    using the JDBC driver, overwriting any existing data in the table.

    Parameters:
        df (DataFrame): The DataFrame to be loaded into the database.
        target_table (str): The name of the target table in the database.

    Raises:
        Exception: If there is an error during the database connection or data
        loading process, an error message is printed.
    """

    DB_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_STAGING}"

    # set config
    connection_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    try:
        # load data
        df.write.jdbc(
            url=DB_URL,
            table=target_table,
            mode="overwrite",
            properties=connection_properties,  # type: ignore
        )

    except Exception as e:
        error_message = str(e)
        print(f"Error connecting to database: {error_message}")
