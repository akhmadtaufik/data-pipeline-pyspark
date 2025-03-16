import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

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

        logging.info("✅ Semua proses selesai!")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
