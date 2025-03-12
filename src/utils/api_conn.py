import os
import requests
from typing import Dict, Any


class APIConnector:
    """
    A class to handle API connections and data retrieval.

    Attributes:
        base_url (str): The base URL for the API, retrieved from the environment variable 'MILESTONES_API'.

    Methods:
        fetch_data(start_date: str, end_date: str) -> Dict[str, Any]:
            Fetches data from the API for the given date range.

    Raises:
        ValueError: If the environment variable 'MILESTONES_API' is not set or if the API URL is not configured.
        SystemExit: If there is a request exception during data fetching.
    """

    def __init__(self) -> None:
        api_url = os.getenv("MILESTONES_API")

        if api_url is None:
            raise ValueError("Environment variable MILESTONES_API is not set")

        self.base_url = api_url

    def fetch_data(self, start_date: str, end_date: str) -> Dict[str, Any]:
        if not self.base_url:
            raise ValueError("API URL is not configured")

        params = {"start_date": start_date, "end_date": end_date}

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException as e:
            raise SystemExit(f"Failed to fetch API data: {e}")
