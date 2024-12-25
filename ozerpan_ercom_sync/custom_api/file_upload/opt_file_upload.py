import logging
import re

import frappe
import pandas as pd
from frappe import _
from typing_extensions import Dict

from ozerpan_ercom_sync.custom_api.utils import get_float_value
from ozerpan_ercom_sync.utils import get_mysql_connection


def process_opt_file(file: Dict, logger: logging.Logger) -> None:
    """Process OPT type Excel file"""
    logger.info("Processing OPT file...")
    try:
        print("\n\n\n")
        print(file)
        print("\n\n\n")
        file_path = file.get("path")
        df = pd.read_excel(file_path)
        if df.empty:
            logger.warning("Empty sheet found.")
            return None

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
        logger.error(f"Error processing OPT file: {str(0)}")
        frappe.throw(_("Error processing OPT file: {0}").format(str(e)))


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
    """Creates or updates an Opt Genel document with item data from dataframe."""
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
            item_code = frappe.db.exists("Item", {stock_code})

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
    """Gets the machine name corresponding to a machine number."""
    machine_names = {2: "Murat TT", 23: "Murat NR242", 24: "Kaban CNC FA-1030"}
    return machine_names.get(machine_no, "")
