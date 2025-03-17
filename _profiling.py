from ast import List
import logging
from datetime import datetime
from src.profiling.profiling import DataProfiler
from src.profiling.extract.extract_csv import extract_csv
from src.profiling.extract.extract_database import (
    extract_table_name,
    extract_databse,
)
from src.profiling.extract.extract_api import extract_api, isEmpty
from src.utils.spark_session import init_spark_session
from src.utils.log import ETLLogger
from src.utils.config import DB_SOURCE

# Initialize Spark
spark = init_spark_session()

# Initialize Logger
logger = ETLLogger()

# Configure Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def profile_all_sources():
    try:
        # ================== Profiling Database ================== #
        db_name = DB_SOURCE
        logging.info(f"Mengambil daftar tabel dari database {db_name}...")

        # Get All Table Names
        tables = extract_table_name(str(db_name))
        if not tables:
            logging.error("Tidak ada tabel yang ditemukan di database!")
            return

        for table in tables:
            try:
                logging.info(f"Memproses tabel: {table}")

                # Extract data from database
                df = extract_databse(str(db_name), table)
                if df is None:
                    continue

                # Profiling
                profiler = DataProfiler(df, table, "postgresql")
                profiler.generate_profile()
                logging.info(f"Profil tabel {table} berhasil dibuat!")

            except Exception as e:
                logging.error(f"Gagal memproses tabel {table}: {str(e)}")
                logger.log(
                    {
                        "step": "profiling",
                        "process": "database",
                        "status": "failed",
                        "source": "postgresql",
                        "table_name": table,
                        "error_msg": str(e),
                        "etl_date": datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                )

        # ================== Profiling CSV ================== #
        csv_files = {
            "people": "data/raw/people.csv",
            "relations": "data/raw/relationships.csv",
        }

        for name, path in csv_files.items():
            try:
                logging.info(f"Memproses CSV: {name}")

                # Extract data from CSV
                df = extract_csv(path)

                # Profiling
                profiler = DataProfiler(df, name, "csv")
                profiler.generate_profile()
                logging.info(f"Profil CSV {name} berhasil dibuat!")

            except Exception as e:
                logging.error(f"Gagal memproses CSV {name}: {str(e)}")
                logger.log(
                    {
                        "step": "profiling",
                        "process": "csv",
                        "status": "failed",
                        "source": "csv",
                        "table_name": name,
                        "error_msg": str(e),
                        "etl_date": datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                )

        # ================== Profiling API ================== #
        try:
            logging.info("Memproses data dari API...")

            # Set Date Ranges
            start_date = "2002-01-01"
            end_date = "2012-12-31"

            # Extract Data from API
            df = extract_api(start_date, end_date)

            if df.isEmpty():
                logging.warning("Tidak ada data dari API")
                return

            # Profiling
            profiler = DataProfiler(df, "milestones", "api")
            profiler.generate_profile()
            logging.info("Profil API milestones berhasil dibuat!")

        except Exception as e:
            logging.error(f"Gagal memproses API: {str(e)}")
            logger.log(
                {
                    "step": "profiling",
                    "process": "api",
                    "status": "failed",
                    "source": "api",
                    "table_name": "milestones",
                    "error_msg": str(e),
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        logging.info("✅ Profiling semua sumber data selesai!")

    except Exception as e:
        logging.error(f"ERROR GLOBAL: {str(e)}")
        logger.log(
            {
                "step": "profiling",
                "process": "full_pipeline",
                "status": "failed",
                "source": "system",
                "table_name": "all",
                "error_msg": str(e),
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )


if __name__ == "__main__":
    profile_all_sources()
