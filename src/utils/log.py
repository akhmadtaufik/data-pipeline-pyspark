import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from typing import Any, Optional
from src.utils.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    DB_LOG,
)


class ETLLogger:
    def __init__(self):
        self.engine = self._create_engine()
        self.schema = "startup"
        self.table_name = "startup_etl_log"

    def _create_engine(self):
        engine = create_engine(
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_LOG}"
        )

        return engine

    def log(self, log_msg: dict[str, Any]):
        try:
            df_log = pd.DataFrame([log_msg])
            df_log.to_sql(
                name=self.table_name,
                con=self.engine,
                schema=self.schema,
                if_exists="append",
                index=False,
            )

        except Exception as e:
            print(f"Gagal menyimpan log: {str(e)}")

    def info(self, log_msg: dict[str, Any]):
        self.log(log_msg)

    def get_last_run(
        self, process_name: str, table_name: str
    ) -> Optional[datetime]:
        query = f"""
        SELECT MAX(etl_date)
        FROM {self.schema}.{self.table_name}
        WHERE process = '{process_name}'
          AND table_name = '{table_name}'
          AND status = 'success'
        """

        try:
            last_run = pd.read_sql_query(query, self.engine).iloc[0, 0]  # type: ignore

            return last_run if last_run else datetime(1900, 1, 1)  # type: ignore

        except Exception as e:
            print(f"Gagal membaca log: {str(e)}")
            return datetime(1900, 1, 1)
