import logging
import re

import frappe
import pandas as pd
from frappe import _

from ozerpan_ercom_sync.custom_api.utils import (
    generate_logger,
    get_file_path,
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
        frappe.ValidationError: If file is not found
        Exception: For other processing errors
    """
    # frappe.publish_progress(25, title="Bar", description="Some Desc")
    logger_dict = generate_logger("opt_genel_update")
    logger = logger_dict["logger"]
    log_file = logger_dict["log_file"]

    try:
        logger.info(f"Starting Opt Genel update process for file: {file_url}")
        file_path = get_file_path(file_url, logger)

        process_opt_genel_excel(file_path, logger)
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


def process_opt_genel_excel(file_path: str, logger: logging.Logger) -> None:
    """Processes Excel file containing Opt Genel data.

    Args:
        file_path: Path to Excel file to process
        logger: Logger instance for tracking operations

    Raises:
        ValueError: If Excel file format is invalid or opt_no cannot be extracted
        frappe.ValidationError: If machine number not found
    """
    logger.info(f"Processing Excel file: {file_path}")

    try:
        df = pd.read_excel(file_path)
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Invalid Excel file format")

        # Extract opt number from fourth column header
        opt_no = extract_opt_no(df.columns[3])
        if not opt_no:
            raise ValueError("Could not extract opt number from Excel file")

        # Get total number of rows in dataframe
        total_rows = len(df)
        logger.info(f"Total rows in dataframe: {total_rows}")

        # Clean and prepare dataframe
        df.columns = df.iloc[1].str.strip()
        df = df.iloc[2:].reset_index(drop=True)

        # Get machine number from database
        machine_no = get_machine_number(opt_no, logger)
        if not machine_no:
            raise frappe.ValidationError("Machine not found")

        create_opt_genel_doc(opt_no, machine_no, df, logger)

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
    opt_no: str, machine_no: int, df: pd.DataFrame, logger: logging.Logger
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
            frappe.get_doc("Opt Genel", opt_no)
            if frappe.db.exists("Opt Genel", opt_no)
            else frappe.new_doc("Opt Genel")
        )

        # Set basic fields
        opt.opt_no = opt_no
        opt.machine_no = get_machine_name(machine_no)

        # Process items
        items_data = []
        for idx, row in df.iterrows():
            # percent = ((idx + 1) / len(df)) * 100
            # frappe.publish_progress(percent, title="Bar", description="Some Desc")
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
