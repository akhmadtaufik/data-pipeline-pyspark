import os
import logging
from dotenv import load_dotenv
from src.extract.extract_csv import extract_csv
from src.extract.extract_api import extract_api
from src.extract.extract_database import extract_databse, extract_table_name
from src.load.load_data import load_data_to_staging

load_dotenv()

# Konfigurasi Logging
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def run_pipeline():
    try:
        # --------------- Extract From Source---------------#
        # 1. Extract CSV
        logging.info("1. Memulai Extract CSV people...")
        people_df = extract_csv("data/raw/people.csv")
        logging.info("2. Memulai Extract CSV relationships...")
        relations_df = extract_csv("data/raw/relationships.csv")

        # 2. Extract Database
        db_name = os.getenv("DB_SOURCE")
        logging.info(f"3. Memulai Extract Database {db_name}...")
        logging.info(f"4. Memulai Extract Nama Tabel from {db_name}...")
        table_names = extract_table_name(db_name)  # type: ignore
        dataframes = {}

        if table_names:
            for table in table_names:
                df = extract_databse(db_name, table)  # type: ignore
                if df is not None:
                    dataframes[table] = df
                    logging.info(f"Berhasil mengekstrak tabel: {table}")
                else:
                    logging.error(f"Gagal mengekstrak tabel: {table}")

        else:
            logging.error("Tidak ada tabel yang ditemukan dalam database.")

        # 3. Extract API
        logging.info("5. Memulai Extract API milestones...")
        start_date = "2000-01-01"
        end_date = "2005-12-31"

        try:
            milestones_df = extract_api(start_date, end_date)

            # Check if DataFrame is empty before attempting to show it
            if milestones_df is not None:
                try:
                    if milestones_df.count() > 0:
                        # Show sample data and schema
                        logging.info("Schema API milestones:")
                        milestones_df.printSchema()
                        logging.info("Sample data API milestones:")
                        milestones_df.show(5, truncate=False)

                    else:
                        logging.warning("Data API milestones kosong. Melewati load ke staging.")
                except Exception as e:
                    logging.error(f"Error saat memproses DataFrame: {str(e)}")
            else:
                logging.warning("Extract API mengembalikan None. Melewati load ke staging.")
        except Exception as e:
            logging.error(f"Error saat mengekstrak data API: {str(e)}")
            logging.warning("Melewati proses load API milestones ke staging karena error.")

        # --------------- Load To Staging ---------------#
        # 1. Load CSV ke Staging
        logging.info("6. Memulai Load CSV people ke staging...")
        load_data_to_staging(people_df, "people")
        logging.info("7. Memulai Load CSV relationships ke staging...")
        load_data_to_staging(relations_df, "relationships")

        # 2. Load Database ke Staging
        for table, df in dataframes.items():
            load_data_to_staging(df, table) # type: ignore
            logging.info(f"Berhasil load tabel {table} ke staging")

        # # 3. Load API ke Staging
        # logging.info("8. Memulai Load API milestones ke staging...")
        # if milestones_df is not None:
        #     load_data_to_staging(milestones_df, "milestones")
        #     logging.info("Berhasil load API milestones ke staging")
        # else:
        #     logging.error(
        #         "Milestones DataFrame kosong. Tidak dapat melakukan load ke staging."
        #     )

        logging.info("✅ Pipeline selesai!")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    run_pipeline()
