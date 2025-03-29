import os
import json
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnull, to_date, min, max
from minio import Minio
from io import BytesIO
from typing import Any
from urllib.parse import urlparse
from src.utils.log_message import log_operation
from src.utils.config import (
    MINIO_ENDPOINT,
    MINIO_PROFILING_BUCKET_NAME,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)


class DataProfiler:
    """
    A class for profiling data from various sources and saving the results to MinIO.

    Attributes:
        data (DataFrame): The data to be profiled.
        table_name (str): The name of the table or dataset.
        source_type (str): The type of data source (e.g., database, CSV, API).
        profile_config (dict): Configuration for profiling based on table name.
        report (dict): A dictionary to store profiling results.
        MINIO_ENDPOINT (str): MinIO server endpoint.
        ACCESS_KEY (str): Access key for MinIO.
        SECRET_KEY (str): Secret key for MinIO.
        BUCKET (str): MinIO bucket name for storing profiling reports.

    Methods:
        _load_profile_config(): Loads profiling configuration for the specified table.
        _log_start(): Logs the start of the profiling process.
        _log_success(): Logs the successful completion of the profiling process.
        _log_error(error_msg): Logs an error during the profiling process.
        _profile_column(col_name, col_type): Profiles a single column based on its type.
        _get_missing_pct(col_name): Calculates the percentage of missing values in a column.
        _get_valid_date_pct(col_name): Calculates the percentage of valid date entries in a column.
        generate_profile(): Generates a profile report for the data and saves it to MinIO.
        _determine_column_type(col_name): Determines the type of a column based on the configuration.
        _save_to_minio(): Saves the profiling report to MinIO.
    """

    def __init__(self, data: DataFrame, table_name: str, source_type: str):
        self.data = data
        self.table_name = table_name
        self.source_type = source_type
        self.profile_config: dict[str, Any] = self._load_profile_config()
        self.report: dict[str, Any] = {
            "table_name": table_name,
            "source_type": source_type,
            "timestamp": datetime.now().isoformat(),
            "columns": {},
        }

        # MinIO Configuration
        self.MINIO_ENDPOINT = MINIO_ENDPOINT
        self.ACCESS_KEY = MINIO_ACCESS_KEY
        self.SECRET_KEY = MINIO_SECRET_KEY
        self.BUCKET = MINIO_PROFILING_BUCKET_NAME

    def _load_profile_config(self) -> dict[str, Any]:
        config = {
            # Database Tables
            "company": {
                "numeric": ["office_id", "latitude", "longitude"],
                "date": ["created_at", "updated_at"],
                "categorical": ["region", "state_code", "country_code"],
                "text": ["description", "address1", "address2"],
            },
            "funds": {
                "numeric": ["fund_id", "raised_amount"],
                "date": ["funded_at", "created_at", "updated_at"],
                "currency": ["raised_currency_code"],
                "text": ["name", "source_description"],
            },
            "acquisition": {
                "numeric": ["acquisition_id", "price_amount"],
                "date": ["acquired_at", "created_at", "updated_at"],
                "currency": ["price_currency_code"],
                "text": ["term_code", "source_description"],
            },
            "funding_rounds": {
                "numeric": [
                    "funding_round_id",
                    "raised_amount_usd",
                    "pre_money_valuation_usd",
                ],
                "date": ["funded_at", "created_at", "updated_at"],
                "currency": [
                    "raised_currency_code",
                    "pre_money_currency_code",
                ],
                "boolean": ["is_first_round", "is_last_round"],
            },
            "investment": {
                "numeric": ["investment_id"],
                "date": ["created_at", "updated_at"],
                "foreign_keys": ["funding_round_id", "funded_object_id"],
            },
            "ipos": {
                "numeric": ["ipo_id", "valuation_amount", "raised_amount"],
                "date": ["public_at", "created_at", "updated_at"],
                "currency": [
                    "valuation_currency_code",
                    "raised_currency_code",
                ],
                "text": ["stock_symbol", "source_description"],
            },
            # CSV Files
            "people": {
                "numeric": ["people_id"],
                "text": [
                    "first_name",
                    "last_name",
                    "birthplace",
                    "affiliation_name",
                ],
                "foreign_keys": ["object_id"],
            },
            "relationships": {
                "numeric": ["relationship_id", "sequence"],
                "date": ["start_at", "end_at", "created_at"],
                "boolean": ["is_past"],
                "text": ["title"],
            },
            # API
            "milestones": {
                "numeric": ["milestone_id"],
                "date": ["milestone_at", "created_at", "updated_at"],
                "text": ["description", "source_description", "source_url"],
                "foreign_keys": ["object_id"],
            },
        }
        return config.get(self.table_name, {})

    def _log_start(self):
        log_operation(
            step="profiling",
            process="profiling",
            status="started",
            source=self.source_type,
            table_name=self.table_name,
            message=f"Memulai profiling {self.table_name}",
        )

    def _log_success(self):
        log_operation(
            step="profiling",
            process="profiling",
            status="success",
            source=self.source_type,
            table_name=self.table_name,
            message=f"Berhasil membuat profil {self.table_name}",
        )

    def _log_error(self, error_msg: str):
        log_operation(
            step="profiling",
            process="profiling",
            status="failed",
            source=self.source_type,
            table_name=self.table_name,
            error_msg=error_msg,
            message=f"Gagal profiling {self.table_name}",
        )

    def _profile_column(self, col_name: str, col_type: str) -> dict[str, Any]:
        profile = {}

        # Basic stats
        profile["data_type"] = str(self.data.schema[col_name].dataType)  # type: ignore
        profile["missing_pct"] = self._get_missing_pct(col_name)

        # Type-specific checks
        if col_type == "numeric":
            profile["min_value"] = self.data.agg({col_name: "min"}).collect()[
                0
            ][0]
            profile["max_value"] = self.data.agg({col_name: "max"}).collect()[
                0
            ][0]
            profile["negative_exists"] = self.data.filter(col(col_name) < 0).count() > 0  # type: ignore

        elif col_type == "date":
            profile["date_format_valid_pct"] = self._get_valid_date_pct(
                col_name
            )

        elif col_type == "categorical":
            profile["unique_count"] = self.data.select(col_name).distinct().count()  # type: ignore
            profile["top_values"] = self.data.groupBy(col_name).count().orderBy("count", ascending=False).limit(5).collect()  # type: ignore

        return profile

    def _get_missing_pct(self, col_name: str) -> float:
        total = self.data.count()
        missing = self.data.filter(col(col_name).isNull()).count()  # type: ignore
        return round((missing / total) * 100, 2) if total > 0 else 0.0

    def _get_valid_date_pct(self, col_name: str) -> float:
        valid = self.data.filter(to_date(col(col_name)).isNotNull()).count()  # type: ignore
        total = self.data.count()
        return round((valid / total) * 100, 2) if total > 0 else 0.0

    def generate_profile(self) -> dict[str, Any]:
        self._log_start()
        try:
            for col_name in self.data.columns:
                col_type = self._determine_column_type(col_name)
                self.report["columns"][col_name] = self._profile_column(col_name, col_type)

            # Save to MinIO
            self._save_to_minio()
            # Save to docs/profiling
            self._save_locally()

            self._log_success()
            return self.report

        except Exception as e:
            self._log_error(str(e))
            raise

    def _determine_column_type(self, col_name: str) -> str:
        """Determine column type based on config"""
        if col_name in self.profile_config.get("numeric_cols", []):
            return "numeric"
        if col_name in self.profile_config.get("date_cols", []):
            return "date"
        if col_name in self.profile_config.get("categorical_cols", []):
            return "categorical"
        return "generic"

    def _save_to_minio(self) -> str:
        """Save report to MinIO"""
        parsed_endpoint = urlparse(str(self.MINIO_ENDPOINT))

        endpoint = parsed_endpoint.netloc if parsed_endpoint.netloc else parsed_endpoint.path

        client = Minio(
            endpoint,
            access_key=self.ACCESS_KEY,
            secret_key=self.SECRET_KEY,
            secure=(parsed_endpoint.scheme == "https"),
        )

        if not client.bucket_exists(str(self.BUCKET)):
            client.make_bucket(str(self.BUCKET))

        report_bytes = json.dumps(self.report).encode("utf-8")
        client.put_object(
            str(self.BUCKET),
            f"{self.table_name}-{datetime.now().strftime('%Y%m%d')}.json",
            BytesIO(report_bytes),
            len(report_bytes),
            "application/json",
        )
        return "Report saved successfully"

    def _save_locally(self) -> None:
        """Save report to local"""
        directory = "docs/profiling"
        if not os.path.exists(directory):
            os.makedirs(directory)

        filename = f"{self.table_name}-{datetime.now().strftime('%Y%m%d')}.json"
        filepath = os.path.join(directory, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.report, f, ensure_ascii=False, indent=4)
