import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
from src.profiling.csv_profiler import CSVProfiler
from src.profiling.database_profiler import DatabaseProfiler
from src.profiling.api_profiler import APIProfiler

load_dotenv()

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    try:
        # Profiling CSV
        PEOPLE_DATA = "data/raw/people.csv"
        RELATIONS_DATA = "data/raw/relationships.csv"

        logging.info("1. Memulai profiling CSV PEOPLE_DATA...")
        # csv_people_profiler = CSVProfiler(PEOPLE_DATA)
        # csv_people_report = csv_people_profiler.profile()
        # csv_people_profiler.save_report(
        #     csv_people_report, "csv_people_profiling"
        # )

        logging.info("2. Memulai profiling CSV RELATIONS_DATA...")
        # csv_relations_profiler = CSVProfiler(RELATIONS_DATA)
        # csv_relations_report = csv_relations_profiler.profile()
        # csv_relations_profiler.save_report(
        #     csv_relations_report, "csv_relati_profiling"
        # )

        # Profiling Database
        # logging.info("3. Memulai profiling database...")
        # db_profiler = DatabaseProfiler("funding_rounds")
        # db_report = db_profiler.profile()
        # db_profiler.save_report(db_report, "db_profiling")

        # Profiling API
        logging.info("4. Memulai profiling API...")
        end_date = "2010-12-31"
        start_date = "2000-01-01"
        api_profiler = APIProfiler(start_date, end_date)
        api_report = api_profiler.profile()
        api_profiler.save_report(api_report, "api_profiling")

        logging.info("✅ Semua proses selesai!")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
