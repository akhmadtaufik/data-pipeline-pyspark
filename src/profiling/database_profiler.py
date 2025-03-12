import os
from typing import Dict, Any
from src.profiling.base_profiler import BaseProfiler
from src.utils.spark_session import init_spark_session


class DatabaseProfiler(BaseProfiler):
    """
    A profiler for database tables that extends the BaseProfiler.

    This class initializes a Spark session and connects to a PostgreSQL database
    to profile a specified table. It retrieves the schema and row count of the table.

    Args:
        table_name (str): The name of the table to be profiled.

    Methods:
        profile: Connects to the database, reads the specified table into a DataFrame,
            and returns a dictionary containing the table's schema and row count.
    """

    def __init__(self, table_name: str) -> None:
        self.spark = init_spark_session()
        self.table_name = table_name

    def profile(self) -> Dict[str, Any]:
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT")
        database = os.getenv("DB_SOURCE")
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")

        # set variable for database
        DB_URL = f"jdbc:postgresql://{host}:{port}/{database}"

        # set config
        jdbc_url = DB_URL
        connection_properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
        }

        try:
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=self.table_name,
                properties=connection_properties,
            )

            return {
                "schema": {f.name: str(f.dataType) for f in df.schema},
                "row_count": df.count(),
            }
        except Exception as e:
            error_message = str(e)
            print(f"Error connecting to database: {error_message}")

            return {
                "error": error_message,
                "connection": {
                    "host": host,
                    "port": port,
                    "database": database,
                    "user": user,
                    "password": password,
                    "status": "failed",
                },
            }
