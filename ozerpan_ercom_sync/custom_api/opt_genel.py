import logging
import re

import frappe
import pandas as pd
from frappe import _

from ozerpan_ercom_sync.custom_api.utils import (
    generate_logger,
    get_file_info,
    get_float_value,
)
from ozerpan_ercom_sync.utils import get_mysql_connection


@frappe.whitelist()
def create_opt_genel(file_url: str) -> dict[str, str]:
    """Updates Opt Genel data from an uploaded file.

    Processes an uploaded Excel file containing Opt Genel data, creating or updating
    corresponding records in the system.

    Args:
        file_url (str): URL of the uploaded file to process

    Returns:
        dict: Status dictionary containing:
            - status (str): "Success" if completed successfully
            - message (str): Description of operation result
            - log_file (str): Path to the generated log file

    Raises:
        frappe.ValidationError: If file is not found or invalid format
        Exception: For other processing errors
    """
    logger_dict = generate_logger("opt_genel_update")
    logger = logger_dict["logger"]
    log_file = logger_dict["log_file"]

    ALLOWED_EXTENSIONS = {".xls", ".xlsx"}

    try:
        logger.info(f"Starting Opt Genel update process for file: {file_url}")
        file = get_file_info(file_url, logger)

        file_extension = file.get("extension", "").lower()
        if file_extension not in ALLOWED_EXTENSIONS:
            raise frappe.ValidationError(
                f"Invalid file format. Allowed formats: {', '.join(ALLOWED_EXTENSIONS)}"
            )

        process_opt_genel_excel(file, logger)
        logger.info("Opt Genel update completed successfully.")

        return {
            "status": "Success",
            "message": _("Opt Genel file processed successfully"),
            "log_file": log_file,
        }

    except frappe.ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error during Opt Genel update: {str(e)}")
        raise


def process_opt_genel_excel(file: dict, logger: logging.Logger) -> None:
    """Processes Excel file containing Opt Genel data.

    Args:
        file: Dictionary containing file information including path and name
        logger: Logger instance for tracking operations

    Raises:
        ValueError: If Excel file format is invalid or opt_no cannot be extracted
        frappe.ValidationError: If file path not found or machine number not found
    """
    logger.info(f"Processing Excel file: {file.get('name')}")

    file_path = file.get("path")
    if not file_path:
        raise frappe.ValidationError("File path not found")

    try:
        # Read Excel file with error handling
        try:
            df = pd.read_excel(file_path)
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {str(e)}")

        if not isinstance(df, pd.DataFrame):
            raise ValueError("Invalid Excel file format")

        # Validate dataframe is not empty
        if df.empty:
            raise ValueError("Excel file is empty")

        # Extract opt number from fourth column header
        opt_no = extract_opt_no(df.columns[3])
        if not opt_no:
            raise ValueError("Could not extract opt number from Excel file")

        opt_code = file.get("code")
        if not opt_code:
            raise ValueError("Could not extract opt code from file")

        # Log data statistics
        total_rows = len(df)
        total_columns = len(df.columns)
        logger.info(f"Dataframe dimensions: {total_rows} rows, {total_columns} columns")

        # Clean and prepare dataframe
        df.columns = df.iloc[1].str.strip()
        df = df.iloc[2:].reset_index(drop=True)

        # Validate cleaned dataframe
        if df.empty:
            raise ValueError("No valid data rows found after cleaning")

        # Get machine number from database
        machine_no = get_machine_number(opt_no, logger)
        if not machine_no:
            raise frappe.ValidationError(f"Machine not found for opt number: {opt_no}")

        create_opt_genel_doc(opt_no, opt_code, machine_no, df, logger)

    except Exception as e:
        logger.error(f"Error processing Excel file: {str(e)}")
        raise


def extract_opt_no(column_header: str) -> str:
    """Extracts opt number from column header string."""
    match = re.match(r"(\d+)", str(column_header))
    return match.group(1) if match else None


def get_machine_number(opt_no: str, logger: logging.Logger) -> int:
    """Gets machine number from database for given opt number."""
    with get_mysql_connection() as connection:
        cursor = connection.cursor()
        query = f"SELECT MAKINA FROM dbtes WHERE OTONO = '{opt_no}'"
        cursor.execute(query)
        machines = cursor.fetchall()
        machine = machines[0] if machines else {}
        return machine.get("MAKINA", 0)


def create_opt_genel_doc(
    opt_no: str, opt_code: str, machine_no: int, df: pd.DataFrame, logger: logging.Logger
) -> None:
    """Creates or updates an Opt Genel document with item data from dataframe.

    Args:
        opt_no: The opt number identifier
        machine_no: The machine number
        df: DataFrame containing item data
        logger: Logger instance for tracking operations

    Raises:
        frappe.ValidationError: If required items are not found
    """
    try:
        # Get existing doc or create new
        opt = (
            frappe.get_doc("Opt Genel", {"opt_no": opt_no})
            if frappe.db.exists("Opt Genel", {"opt_no": opt_no})
            else frappe.new_doc("Opt Genel")
        )

        # Set basic fields
        opt.opt_no = opt_no
        opt.opt_code = opt_code
        opt.machine_no = get_machine_name(machine_no)

        # Process items
        items_data = []
        for idx, row in df.iterrows():
            stock_code = str(row["Stok Kodu"]).strip()
            item_code = frappe.db.exists("Item", f"erc-{stock_code}")

            if not item_code:
                logger.error(f"Item not found for stock code: {stock_code}")
                frappe.throw(f"Item not found for stock code: {stock_code}")

            items_data.append(
                {
                    "item_code": item_code,
                    "item_name": str(row["Açıklama"]),
                    "amountboy": get_float_value(str(row["Adet"])),
                    "amountmt": get_float_value(str(row["Kullanılan"])),
                    "amountpcs": get_float_value(str(row["Parça"])),
                }
            )

        opt.set("profile_list", items_data)
        opt.save(ignore_permissions=True)
        logger.info(
            f"Successfully {'updated' if opt.name else 'created'} Opt Genel: {opt.name}"
        )

    except Exception as e:
        logger.error(f"Error creating/updating Opt Genel doc: {str(e)}")
        raise


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
