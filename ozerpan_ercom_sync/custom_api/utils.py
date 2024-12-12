import logging
import os
from datetime import datetime

import frappe


def get_float_value(value: str) -> float:
    """Converts a string value to float by cleaning currency symbols and standardizing decimal format.

    Converts a string containing a number (potentially with currency symbol 'tl') to a float value
    by removing the currency symbol, standardizing decimal separator, and converting to float.

    Args:
        value (str): String value to convert, may contain 'tl' currency symbol and number

    Returns:
        float: Cleaned and converted float value

    Example:
        >>> get_float_value("123,45 tl")
        123.45
        >>> get_float_value("1.234,56")
        1234.56
    """
    cleaned_value = (
        value.lower().replace("tl", "").strip().replace(".", "").replace(",", ".")
    )
    return float(cleaned_value)


def generate_logger(log_name: str) -> dict[str, logging.Logger | str]:
    """Generates and configures a logger with file output.

    Creates a logger that writes to a timestamped log file in a subdirectory under
    the site's logs directory. Configures basic logging settings.

    Args:
        log_name (str): Base name to use for logger and log file/directory

    Returns:
        dict: Dictionary containing:
            - logger (logging.Logger): Configured logger instance
            - log_file (str): Full path to the generated log file

    Example:
        >>> logger_dict = generate_logger("process")
        >>> logger = logger_dict["logger"]
        >>> log_file = logger_dict["log_file"]
        >>> logger.info("Starting process") # Writes to log file
    """
    log_dir: str = os.path.join(frappe.get_site_path(), "logs", f"{log_name}s")
    os.makedirs(log_dir, exist_ok=True)

    timestamp: str = datetime.now().strftime("%Y%m%s_%H%M%S")
    log_file: str = os.path.join(log_dir, f"{log_name}_{timestamp}.log")

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger: logging.Logger = logging.getLogger(__name__)
    return {"logger": logger, "log_file": log_file}


def get_file_path(file_url: str, logger: logging.Logger) -> str:
    """Constructs and validates the physical file path from URL.

    Args:
        file_url: URL of the file to locate
        logger: Logger instance for tracking operations

    Returns:
        str: Physical path to the file

    Raises:
        frappe.ValidationError: If file not found
    """
    file_doc = frappe.get_doc("File", {"file_url": file_url})
    file_location = "private" if file_doc.is_private else "public"
    file_name = os.path.basename(file_url)
    file_path = frappe.get_site_path(file_location, "files", file_name)

    if not os.path.exists(file_path):
        msg = f"File not found: {file_path}"
        logger.error(msg)
        frappe.throw(msg)

    return file_path
