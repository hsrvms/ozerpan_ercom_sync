import logging
import os
from datetime import datetime

import frappe
from ozerpan_ercom_sync.utils import get_mysql_connection


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


def get_file_info(file_url: str, logger: logging.Logger) -> dict[str, str]:
    """Constructs and validates file information from URL.

    Args:
        file_url: URL of the file to locate
        logger: Logger instance for tracking operations

    Returns:
        dict: Dictionary containing file information:
            - file_path: Physical path to the file
            - file_name: File name without extension
            - file_extension: File extension
            - code: First part of file name before underscore
            - category: Second part of file name after underscore

    Raises:
        frappe.ValidationError: If file not found
        IndexError: If file name does not contain expected parts
    """
    file_doc = frappe.get_doc("File", {"file_url": file_url})
    file_location = "private" if file_doc.is_private else "public"

    # Extract file components
    file_name = os.path.basename(file_url)
    file_name_without_extension, file_extension = os.path.splitext(file_name)

    try:
        code, category = file_name_without_extension.split("_")
    except ValueError as e:
        msg = f"Invalid file name format: {file_name_without_extension}. Expected format: code_category"
        logger.error(msg)
        raise ValueError(msg) from e

    # Validate file exists
    file_path = frappe.get_site_path(file_location, "files", file_name)
    if not os.path.exists(file_path):
        msg = f"File not found: {file_path}"
        logger.error(msg)
        frappe.throw(msg)

    return {
        "path": file_path,
        "name": file_name_without_extension,
        "extension": file_extension,
        "code": code,
        "category": category,
    }


def check_file_type(extension: str, file_type: str) -> None:
    """Validates file extension against allowed types.

    Args:
        extension: File extension including dot (e.g. '.xlsx')
        file_type: Type of file to validate against ('excel', etc)

    Raises:
        frappe.ValidationError: If extension not allowed for file_type
    """
    ALLOWED_EXTENSIONS = {"excel": {".xls", ".xlsx"}}

    if file_type in ALLOWED_EXTENSIONS:
        allowed = ALLOWED_EXTENSIONS[file_type]
        if extension.lower() not in allowed:
            raise frappe.ValidationError(
                f"Invalid file format. Allowed formats: {', '.join(allowed)}"
            )
    else:
        raise frappe.ValidationError(
            f"Invalid file type. Allowed types: {', '.join(ALLOWED_EXTENSIONS.keys())}"
        )


def show_progress(curr_count: int, max_count: int, title: str, desc: str):
    percent = curr_count * 100 / max_count
    frappe.publish_progress(
        percent,
        title=title,
        description=desc,
    )



def get_machine_number(opt_no: str, logger: logging.Logger) -> int:
    """Gets machine number from database for given opt number."""
    with get_mysql_connection() as connection:
        with connection.cursor() as cursor:
            query = f"SELECT MAKINA FROM dbtes WHERE OTONO = '{opt_no}'"
            cursor.execute(query)
            machines = cursor.fetchall()
            machine = machines[0] if machines else {}
            return machine.get("MAKINA", 0)


def get_machine_name(machine_no: int) -> str:
    """Gets the machine name corresponding to a machine number.

    Args:
        machine_no: Integer identifier for the machine

    Returns:
        str: Name of the machine if found, empty string if not found

    Example:
        >>> get_machine_name(2)
        'Murat TT'
    """
    machine_names = {2: "Murat TT", 23: "Murat NR242", 24: "Kaban CNC FA-1030"}
    return machine_names.get(machine_no, "")
