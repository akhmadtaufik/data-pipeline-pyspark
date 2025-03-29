from src.profiling.profiling import DataProfiler
from src.profiling.extract.extract_csv import extract_csv
from src.profiling.extract.extract_database import (
    extract_table_name,
    extract_databse,
)
from src.profiling.extract.extract_api import extract_api
from src.utils.spark_session import init_spark_session
from src.utils.config import DB_SOURCE
from src.utils.log_message import log_operation

# Initialize Spark
spark = init_spark_session()


def profile_all_sources():
    try:
        # ================== Profiling Database ================== #
        db_name = DB_SOURCE
        tables = extract_table_name(str(db_name))
        for table in tables:  # type: ignore
            df = extract_databse(str(db_name), table)
            DataProfiler(df, table, "database").generate_profile()

        # ================== Profiling CSV ================== #
        csv_files = {
            "people": "data/raw/people.csv",
            "relations": "data/raw/relationships.csv",
        }

        for name, path in csv_files.items():
            df = extract_csv(path)
            DataProfiler(df, name, "csv").generate_profile()  # type: ignore

        # ================== Profiling API ================== #
        df = extract_api("2002-01-01", "2012-12-31")
        if not df.isEmpty():
            DataProfiler(df, "milestones", "api").generate_profile()

    except Exception as e:
        log_operation(
            step="system",
            process="profiling",
            status="failed",
            source="system",
            table_name="all",
            error_msg=str(e),
        )


if __name__ == "__main__":
    profile_all_sources()
