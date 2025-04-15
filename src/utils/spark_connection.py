from pyspark.sql import DataFrame
from src.utils.spark_session import init_spark_session
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


def write_jdbc(df: DataFrame, db: str, schema: str, table_name: str) -> None:
    """
    Writes the given DataFrame to a PostgreSQL database table using JDBC.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to be written to the database.
    db : str
        The name of the database to connect to.
    schema : str
        The schema within the database where the table resides.
    table_name : str
        The name of the table to write the DataFrame to.

    Returns
    -------
    None
    """
    df.write.jdbc(
        url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db}",
        table=f"{schema}.{table_name}",
        mode="append",
        properties={
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver",
        },  # type: ignore
    )
