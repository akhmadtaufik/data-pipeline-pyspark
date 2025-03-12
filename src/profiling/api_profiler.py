import json
from datetime import datetime
from typing import Any, Dict, List, Optional, TypedDict, cast
from src.profiling.base_profiler import BaseProfiler
from src.utils.api_conn import APIConnector


class MilestoneData(TypedDict):
    """
    A TypedDict representing milestone data with attributes for milestone time, code,
    and an optional object identifier.

    Attributes:
        milestone_at (str): The timestamp when the milestone was achieved.
        milestone_code (str): A code representing the specific milestone.
        object_id (Optional[str]): An optional identifier for the related object.
    """

    milestone_at: str
    milestone_code: str
    object_id: Optional[str]


class APIProfiler(BaseProfiler):
    """
    A profiler class that extends BaseProfiler to analyze milestone data fetched
    from an API within a specified date range.

    Attributes:
        api (APIConnector): An instance of APIConnector to handle API interactions.
        start_date (str): The start date for fetching milestone data.
        end_date (str): The end date for fetching milestone data.

    Methods:
        profile() -> Dict[str, Any]: Fetches milestone data from the API and returns
        a profiling report containing total records, date range, unique milestone
        codes, and missing object IDs.
    """

    def __init__(self, start_date: str, end_date: str) -> None:
        self.api = APIConnector()
        self.start_date = start_date
        self.end_date = end_date

    def profile(self) -> Dict[str, Any]:
        data: List[MilestoneData] = cast(
            List[MilestoneData],
            self.api.fetch_data(self.start_date, self.end_date),
        )

        return {
            "total_records": len(data),
            "date_range": {
                "start": min([d["milestone_at"] for d in data]),
                "end": max([d["milestone_at"] for d in data]),
            },
            "columns": {
                "milestone_code": {
                    "unique_values": len(
                        set(d["milestone_code"] for d in data)
                    )
                },
                "object_id": {
                    "missing_values": sum(
                        1 for d in data if not d.get("object_id")
                    )
                },
            },
        }
