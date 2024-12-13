import logging
import os
from datetime import datetime

import frappe
import pandas as pd
from frappe import _

from ozerpan_ercom_sync.custom_api.utils import show_progress
from ozerpan_ercom_sync.utils import get_mysql_connection


@frappe.whitelist()
def update_bom(file_url):
    """Updates Bill of Materials (BOM) from an uploaded Excel file.

    Sets up logging, processes the Excel file to update BOM details, and handles any errors.

    Args:
        file_url (str): URL path to the uploaded Excel file

    Returns:
        dict: Status dictionary containing:
            - status (str): "Success" if completed successfully
            - message (str): Empty string if successful, error message if failed
            - log_file (str): Path to the generated log file

    Raises:
        Exception: If file is not found or other errors occur during processing

    Example:
        result = update_bom("/files/bom_update.xlsx")
        if result["status"] == "Success":
            # BOM updated successfully
    """
    # Setup logging
    log_dir = os.path.join(frappe.get_site_path(), "logs", "bom_updates")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir, f'bom_update_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    )

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(__name__)

    try:
        logger.info(f"Starting BOM update process for file: {file_url}")
        file_doc = frappe.get_doc("File", {"file_url": file_url})

        file_path = frappe.get_site_path(
            "private" if file_doc.is_private else "public",
            "files",
            os.path.basename(file_url),
        )

        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            frappe.throw(f"File not found: {file_path}")

        process_excel(file_path, logger)
        logger.info("BOM update completed successfully")
        return {"status": "Success", "message": "", "log_file": log_file}

    except Exception as e:
        logger.error(f"Error during BOM update: {str(e)}")
        raise


def process_excel(file_path, logger):
    """Processes Excel file to update BOM information.

    Reads Excel file sheets, extracts BOM details and raw materials, and updates
    corresponding BOM records in the system.

    Args:
        file_path (str): Path to the Excel file to process
        logger (logging.Logger): Logger instance for logging operations

    Raises:
        Exception: If BOM does not exist for an item code

    Example:
        logger = logging.getLogger(__name__)
        process_excel("path/to/excel.xlsx", logger)
    """
    ef = pd.ExcelFile(file_path)
    sheets = ef.sheet_names
    logger.info(f"Processing Excel file with {len(sheets)} sheets")

    order_no = pd.read_excel(file_path).tail(3)["Stok Kodu"].iloc[0]

    connection = get_mysql_connection()
    cursor = connection.cursor()
    query: str = f"SELECT * FROM dbsiparis WHERE SIPARISNO = '{order_no}'"
    cursor.execute(query)
    orders = cursor.fetchall()
    order = orders[0] if orders else {}  # Take first order or empty dict if none
    cursor.close()
    connection.close()

    so = frappe.new_doc("Sales Order")
    if frappe.db.exists("Sales Order", {"custom_ercom_order_no": order_no}):
        existing_so = frappe.get_doc("Sales Order", {"custom_ercom_order_no": order_no})
        if existing_so.docstatus.is_submitted():
            logger.info("Cancelling submitted Sales Order")
            existing_so.cancel()
            so = frappe.new_doc("Sales Order")
        elif existing_so.docstatus.is_draft():
            so = existing_so
            so.items = []
            so.taxes = []
        elif existing_so.docstatus.is_cancelled():
            so = frappe.new_doc("Sales Order")

    so.customer = order.get("CARIUNVAN")
    so.date = order.get("SIPTARIHI")
    so.delivery_date = order.get("SEVKTARIHI")
    so.company = frappe.defaults.get_user_default("company")
    so.order_type = "Sales"
    so.custom_ercom_order_no = order_no
    tax_account = get_tax_account()
    so.append(
        "taxes",
        {
            "charge_type": "On Net Total",
            "account_head": tax_account.get("name"),
            "rate": tax_account.get("tax_rate"),
            "description": tax_account.get("name"),
        },
    )

    dtype_dict = {"Stok Kodu": str, "Toplam Fiyat": str}
    sheets_len = len(sheets)

    for i, sheet in enumerate(sheets):
        progress_title = "Sales Order Item Sync"
        progress_desc = _("Syncing Sheet {0} of {1}").format(i + 1, sheets_len)
        show_progress(i + 1, sheets_len, progress_title, progress_desc)
        logger.info(f"Processing sheet: {sheet}")
        df = pd.read_excel(file_path, sheet, dtype=dtype_dict)
        if df.empty:
            logger.warning(f"Empty sheet found: {sheet}")
            continue

        tail = df.tail(3).copy()
        item_code = f"{tail['Stok Kodu'].iloc[0]}-{tail['Stok Kodu'].iloc[1]}"
        logger.info(f"Processing item code: {item_code}")

        if not frappe.db.exists("BOM", {"item": item_code}):
            logger.error(f"BOM not found for item: {item_code}")
            frappe.throw(f"BOM for the Item ({item_code}) does not exist.")

        bom_doc = frappe.get_doc("BOM", {"item": item_code})

        filtered_df = df[df["Stok Kodu"].str.startswith("#", na=False)].copy()
        logger.info(f"Found {len(filtered_df)} raw materials for BOM")

        update_bom_raw_materials(bom_doc, filtered_df, logger)
        item = update_bom_item_valuation_rate(
            item_code, tail["Toplam Fiyat"].iloc[0], logger
        )
        so.append(
            "items",
            {
                "item_code": item.item_code,
                "delivery_date": so.get("delivery_date"),
                "item_name": item.item_name,
                "description": item.description if item.description else item.name,
                "item_group": item.item_group,
                "qty": float(item.custom_quantity),
                "uom": item.stock_uom,
                "conversion_factor": 1,
                "rate": item.valuation_rate,
            },
        )
    so.save(ignore_permissions=True)
    so.submit()


def get_tax_account():
    if not frappe.db.exists(
        "Account", {"account_name": "ERCOM HESAPLANAN KDV 20", "account_number": "391.99"}
    ):
        company = frappe.get_doc("Company", frappe.defaults.get_user_default("company"))
        ta = frappe.new_doc("Account")
        ta.account_name = "ERCOM HESAPLANAN KDV 20"
        ta.account_number = "391.99"
        ta.parent_account = f"391 - HESAPLANAN KDV - {company.abbr}"
        ta.currency = "TRY"
        ta.account_type = "Tax"
        ta.tax_rate = 20
        ta.save(ignore_permissions=True)
        return ta
    else:
        return frappe.get_doc(
            "Account",
            {"account_name": "ERCOM HESAPLANAN KDV 20", "account_number": "391.99"},
        )


def update_bom_item_valuation_rate(item_code, total_amount, logger):
    """Updates the valuation rate for an item in the BOM.

    Updates the valuation rate of an existing item in the Bill of Materials (BOM)
    with a new total amount value.

    Args:
        item_code (str): Code identifying the item to update
        total_amount (str): New total amount/valuation rate to set
        logger (logging.Logger): Logger instance for logging operations

    Raises:
        frappe.DoesNotExistError: If item with given item_code does not exist

    Example:
        logger = logging.getLogger(__name__)
        update_bom_item_valuation_rate("ITEM-001", "1000.00", logger)
    """
    logger.info(f"Updating valuation rate for item: {item_code}")
    if not frappe.db.exists("Item", {"item_code": item_code}):
        logger.error(f"Item not found: {item_code}")
        frappe.throw(f"Item ({item_code}) does not exist")

    item = frappe.get_doc("Item", {"item_code": item_code})
    item.valuation_rate = get_float_value(total_amount)
    item.save(ignore_permissions=True)
    logger.info(f"Updated valuation rate to {item.valuation_rate}")
    return item


def update_bom_raw_materials(doc: "frappe.Document", df: pd.DataFrame, logger) -> None:
    """Updates raw materials in a Bill of Materials document.

    Cancels submitted BOM if needed, processes raw materials from DataFrame,
    calculates quantities and rates, and submits updated BOM.

    Args:
        doc (frappe.Document): BOM document to update
        df (pd.DataFrame): DataFrame containing raw material details
        logger (logging.Logger): Logger instance for logging operations

    Returns:
        None

    Raises:
        frappe.ValidationError: If BOM update fails

    Example:
        bom_doc = frappe.get_doc("BOM", "BOM-0001")
        materials_df = pd.DataFrame(...)
        logger = logging.getLogger(__name__)
        update_bom_raw_materials(bom_doc, materials_df, logger)
    """
    logger.info(f"Updating BOM raw materials for {doc.name}")
    if doc.docstatus.is_submitted():
        logger.info("Cancelling submitted BOM")
        doc.cancel()
        bom_doc = frappe.copy_doc(doc)
        bom_doc.amended_from = doc.name
    else:
        bom_doc = doc

    items_data = []
    for _, row in df.iterrows():
        stock_code = row["Stok Kodu"].lstrip("#")
        logger.info(f"Processing raw material: {stock_code}")
        item = create_or_update_raw_material_item(stock_code, row, logger)
        rate = float(item.get("valuation_rate", 0.0))
        amount = get_float_value(str(row.get("Toplam Fiyat", "0.0")))
        qty = (
            round((amount / rate), 7)
            if rate != 0.0
            else get_float_value(row.get("Miktar"))
        )

        items_data.append(
            {
                "item_code": str(item.get("item_code")),
                "item_name": str(item.get("item_name")),
                "description": str(item.get("description")),
                "uom": str(item.get("stock_uom")),
                "stock_uom": str(item.get("stock_uom")),
                "qty": qty,
                "rate": rate,
            }
        )

    bom_doc.set("items", items_data)
    bom_doc.save(ignore_permissions=True)
    bom_doc.submit()
    logger.info(f"BOM {bom_doc.name} updated and submitted successfully")


def create_or_update_raw_material_item(
    stock_code: str, data: pd.Series, logger
) -> "frappe.Document":
    """Creates or updates a raw material item in the system.

    Takes a stock code and associated data and either creates a new raw material item
    or updates an existing one with the provided information.

    Args:
        stock_code (str): The stock code identifier for the raw material
        data (pd.Series): Series containing item data with keys:
            - Açıklama: Item description
            - Birim: Unit of measure
            - Birim Fiyat: Unit price
            - Birim Kg.: Weight per unit
        logger (logging.Logger): Logger instance for logging operations

    Returns:
        frappe.Document: The created or updated Item document

    Raises:
        frappe.ValidationError: If item creation/update fails

    Example:
        data = pd.Series({
            'Açıklama': 'Steel Bar',
            'Birim': 'kg',
            'Birim Fiyat': '100.00',
            'Birim Kg.': '1.5'
        })
        item = create_or_update_raw_material_item('1234', data, logger)
    """
    item_code = f"erc-{stock_code}"
    logger.info(f"Creating/Updating raw material item: {item_code}")

    description = str(data.get("Açıklama", ""))
    stock_uom = get_uom(str(data.get("Birim", "")))

    if frappe.db.exists("Item", {"item_code": item_code}):
        logger.info(f"Updating existing item: {item_code}")
        item = frappe.get_doc("Item", {"item_code": item_code})
    else:
        logger.info(f"Creating new item: {item_code}")
        item = frappe.new_doc("Item")
        item.item_code = item_code
        item.item_group = "Raw Material"

    item.item_name = description
    item.description = description
    item.stock_uom = stock_uom
    item.valuation_rate = get_float_value(str(data.get("Birim Fiyat", "0.0")))
    item.weight_per_unit = get_float_value(str(data.get("Birim Kg.", "0.0")))

    item.save(ignore_permissions=True)
    return item


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


def get_uom(unit: str) -> str:
    """Gets or creates a Unit of Measure (UOM) record.

    Takes a unit string, maps it to a standardized UOM name using UOM_MAP,
    and either returns existing UOM or creates new one.

    Args:
        unit (str): Unit string to convert to standard UOM

    Returns:
        str: Name of existing or newly created UOM record

    Example:
        >>> get_uom("kg")
        "Kilogram"
        >>> get_uom("mtül")
        "Mtul"
    """
    UOM_MAP = {
        "mtül": "Mtul",
        "adet": "Adet",
        "m²": "Square Meter",
        "kg": "Kilogram",
        "litre": "Litre",
        "kutu": "Box",
        "tane": "Tane",
    }

    unit_lower = str(unit).lower()
    uom = UOM_MAP.get(unit_lower, "Other")

    if frappe.db.exists("UOM", uom):
        return uom

    new_uom = frappe.new_doc("UOM")
    new_uom.uom_name = uom
    new_uom.enabled = 1
    new_uom.save(ignore_permissions=True)
    return new_uom.name
