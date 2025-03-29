import logging
from datetime import datetime
from src.utils.log import ETLLogger

logger = ETLLogger()

def log_operation(
    step: str,
    process: str,
    status: str,
    source: str,
    table_name: str,
    error_msg: str = None,  # type: ignore
    message: str = None,  # type: ignore
) -> None:
    """
    Logs an operation's details to both the ETL log database and the standard logging system.

    This function creates a log entry with details about a specific ETL operation step, including
    the step name, process name, status, source, and table name. It logs this entry to the ETL log
    database using the ETLLogger class and also logs a formatted message to the standard logging
    system.

    Parameters:
    - step (str): The name of the ETL step being logged.
    - process (str): The name of the ETL process.
    - status (str): The status of the operation (e.g., 'success', 'failure').
    - source (str): The source of the data being processed.
    - table_name (str): The name of the table involved in the operation.
    - error_msg (str, optional): An optional error message if the operation failed.
    - message (str, optional): An optional additional message to include in the log.

    Returns:
    - None
    """
    log_entry = {
        "step": step,
        "process": process,
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
