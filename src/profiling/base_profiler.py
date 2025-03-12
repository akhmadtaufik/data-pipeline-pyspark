import json
from typing import Any, Dict
from abc import ABC, abstractmethod


class BaseProfiler(ABC):
    """
    An abstract base class for profiling tasks.

    Methods:
        profile: Abstract method that should be implemented to perform profiling
            and return a report as a dictionary.
        save_report: Saves the profiling report to a JSON file in the 'docs' directory.

    Args:
        report (Dict[str, Any]): The profiling report to be saved.
        filename (str): The name of the file to save the report as, without extension.
    """

    @abstractmethod
    def profile(self) -> Dict[str, Any]:
        pass

    def save_report(self, report: Dict[str, Any], filename: str):
        with open(f"docs/{filename}.json", "w") as f:
            json.dump(report, f, indent=2)
