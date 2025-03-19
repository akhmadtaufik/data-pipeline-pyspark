import logging
from datetime import datetime
from src.utils.log import ETLLogger

logger = ETLLogger()


def log_operation(
    step: str,
    status: str,
    source: str,
    table_name: str,
    error_msg: str = None,  # type: ignore
    message: str = None,  # type: ignore
) -> None:
    """
    Logs the operation details of an ETL step to the ETLLogger and outputs
    a formatted log message.

    Args:
        step (str): The current step in the ETL process.
        status (str): The status of the operation (e.g., 'started', 'success', 'failed').
        source (str): The source of the data being processed.
        table_name (str): The name of the table involved in the operation.
        error_msg (str, optional): An error message if the operation failed.
        message (str, optional): Additional message to include in the log.

    Returns:
        None
    """
    log_entry = {
        "step": step,
        "process": "staging",
        "status": status,
        "source": source,
        "table_name": table_name,
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    if error_msg:
        log_entry["error_msg"] = error_msg

    logger.log(log_entry)

    log_msg = f"{step.upper()} {status.upper()} - {source}.{table_name}"

    if message:
        log_msg += f" | {message}"

    logging.info(log_msg)
