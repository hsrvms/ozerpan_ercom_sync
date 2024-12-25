import logging
from typing import Dict, List, Optional, Tuple

import frappe
import pandas as pd
from frappe import _

from ozerpan_ercom_sync.custom_api.utils import (
    get_float_value,
    show_progress,
)
from ozerpan_ercom_sync.utils import get_mysql_connection

DEFAULT_TAX_ACCOUNT = {
    "name": "ERCOM HESAPLANAN KDV 20",
    "number": "391.99",
    "tax_rate": 20,
}


def process_mly_file(file: Dict, logger: logging.Logger) -> None:
    """Process MLY type Excel file"""
    logger.info("Processing MLY file...")
    try:
        df, sheets = read_excel_file(file["path"])
        order_no = extract_order_number(df)
        poz_data = get_poz_data(order_no, logger)

        sales_order = get_sales_order(order_no, logger)
        update_sales_order_taxes(sales_order)

        so_items = process_sheets(file["path"], sheets, poz_data, logger)
        update_sales_order_items(sales_order, so_items)

    except Exception as e:
        logger.error(f"Error processing MLY file: {str(e)}")
        frappe.throw(_("Error processing MLY file: {0}").format(str(e)))


def read_excel_file(file_path: str) -> Tuple[pd.DataFrame, List[str]]:
    """Read Excel file and validate content"""
    try:
        df = pd.read_excel(file_path)
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Invalid Excel file format.")
        if df.empty:
            raise ValueError("Excel file is empty.")

        ef = pd.ExcelFile(file_path)
        return df, ef.sheet_names
    except Exception as e:
        raise ValueError(f"Failed to read Excel file: {str(e)}")


def extract_order_number(df: pd.DataFrame) -> str:
    """Extract order number from dataframe"""
    return df.tail(3)["Stok Kodu"].iloc[0]


def get_sales_order(order_no: str, logger: logging.Logger):
    """Fetch sales order document"""
    if not frappe.db.exists(
        "Sales Order",
        {
            "custom_ercom_order_no": order_no,
            "status": "Draft",
        },
    ):
        logger.error("Sales Order not found.")
        raise ValueError(
            "No such Sales Order found. Please sync the database before uploading the file."
        )
    return frappe.get_last_doc(
        "Sales Order",
        {
            "custom_ercom_order_no": order_no,
            "status": "Draft",
        },
    )


def update_sales_order_taxes(sales_order) -> None:
    """Update sales order tax information"""
    tax_account = get_tax_account()

    # Check if tax account already exists in taxes
    existing_tax = next(
        (tax for tax in sales_order.taxes if tax.account_head == tax_account.get("name")),
        None,
    )

    if not existing_tax:
        sales_order.append(
            "taxes",
            {
                "charge_type": "On Net Total",
                "account_head": tax_account.get("name"),
                "rate": tax_account.get("tax_rate"),
                "description": tax_account.get("name"),
            },
        )


def process_sheets(
    file_path: str, sheets: List[str], poz_data: List[Dict], logger: logging.Logger
) -> List[Dict]:
    """Process all sheets in Excel file"""
    dtype_dict = {"Stok Kodu": str, "Toplam Fiyat": str}
    so_item_table = []

    for i, sheet in enumerate(sheets):
        show_progress(
            curr_count=i + 1,
            max_count=len(sheets),
            title=_("Sales Order File Upload"),
            desc=_("Syncing Sheet {0} of {1}").format(i + 1, len(sheets)),
        )
        logger.info(f"Processing sheet: {sheet}")

        try:
            sheet_data = process_single_sheet(
                file_path, sheet, dtype_dict, poz_data[i], logger
            )
        except IndexError:
            logger.warning(f"Skipping empty sheet {sheet} - no matching poz_data index")
            continue
        if sheet_data:
            so_item_table.append(sheet_data)

    return so_item_table


def process_single_sheet(
    file_path: str, sheet: str, dtype_dict: Dict, poz_data: Dict, logger: logging.Logger
) -> Optional[Dict]:
    """Process individual sheet from Excel file"""
    df = pd.read_excel(file_path, sheet, dtype=dtype_dict)
    if df.empty:
        logger.warning(f"Empty sheet found: {sheet}")
        return None

    tail = df.tail(3).copy()
    filtered_df = df[df["Stok Kodu"].str.startswith("#", na=False)].copy()

    item_code = f"{tail['Stok Kodu'].iloc[0]}-{tail['Stok Kodu'].iloc[1]}"
    total_price = tail["Toplam Fiyat"].iloc[0]

    logger.info(f"Processing item: {item_code}")

    item = create_item(item_code, total_price, poz_data, logger)
    bom_result = create_bom(item.name, poz_data.get("ADET"), filtered_df, logger)

    return {
        "item_code": item.item_code,
        "item_name": item.item_name,
        "description": item.description,
        "qty": item.custom_quantity,
        "uom": item.stock_uom,
        "rate": bom_result.get("total_cost"),
    }


def update_sales_order_items(sales_order, items: List[Dict]) -> None:
    """Update sales order with processed items"""
    sales_order.set("items", items)
    sales_order.save(ignore_permissions=True)


def get_poz_data(order_no: str, logger: logging.Logger) -> List[Tuple]:
    """Get order data from dbpoz table"""
    try:
        with get_mysql_connection() as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT SAYAC, SIPARISNO, GENISLIK, YUKSEKLIK, ADET,
                    SERI, ACIKLAMA, NOTLAR, PozID
                    FROM dbpoz WHERE SIPARISNO = %s
                """
                cursor.execute(query, (order_no,))
                results = cursor.fetchall()
                logger.info(f"Retrieved {len(results)} records for order {order_no}")
                return results
    except Exception as e:
        error_msg = f"Error fetching data for order {order_no}: {str(e)}"
        logger.error(error_msg)
        frappe.log_error(error_msg)
        raise frappe.ValidationError(error_msg)


def create_bom(
    item_name: str, qty: float, df: pd.DataFrame, logger: logging.Logger
) -> Dict:
    """Create Bill of Materials document"""
    company = frappe.defaults.get_user_default("Company")
    bom = frappe.new_doc("BOM")
    bom.item = item_name
    bom.company = company
    bom.quantity = qty
    bom.rm_cost_as_per = "Price List"
    bom.buying_price_list = "Standard Selling"

    items_table = []
    for _, row in df.iterrows():
        stock_code = row["Stok Kodu"].lstrip("#")
        if not frappe.db.exists("Item", stock_code):
            raise ValueError(f"No such item: {stock_code}")

        item = frappe.get_doc("Item", stock_code)
        if not item.custom_kit:
            items_table.append(create_bom_item(row, item))

    bom.set("items", items_table)
    bom.save(ignore_permissions=True)
    bom.submit()

    logger.info(f"Created BOM for item {item_name}")
    return {
        "msg": "BOM created successfully.",
        "docname": bom.name,
        "total_cost": bom.total_cost,
    }


def create_bom_item(row: pd.Series, item) -> Dict:
    """Create BOM item entry"""
    rate = get_float_value(str(row.get("Birim Fiyat", "0.0")))
    amount = get_float_value(str(row.get("Toplam Fiyat", "0.0")))
    item_qty = (
        round((amount / rate), 7) if rate != 0.0 else get_float_value(row.get("Miktar"))
    )

    return {
        "item_code": item.get("item_code"),
        "item_name": item.get("item_name"),
        "description": item.get("description"),
        "uom": str(row.get("Birim")),
        "qty": item_qty,
        "rate": rate,
    }


def create_item(
    item_code: str, total_price: float, poz_data: Dict, logger: logging.Logger
):
    """Create or update Item document"""
    if frappe.db.exists("Item", {"item_code": item_code}):
        item = frappe.get_doc("Item", {"item_code": item_code})
    else:
        item = frappe.new_doc("Item")

    item.update(
        {
            "item_code": item_code,
            "item_name": item_code,
            "item_group": "All Item Groups",
            "stock_uom": "Nos",
            "valuation_rate": total_price,
            "description": poz_data.get("ACIKLAMA"),
            "custom_serial": poz_data.get("SERI"),
            "custom_width": poz_data.get("GENISLIK"),
            "custom_height": poz_data.get("YUKSEKLIK"),
            "custom_color": poz_data.get("RENK"),
            "custom_quantity": poz_data.get("ADET"),
            "custom_remarks": poz_data.get("NOTLAR"),
            "custom_poz_id": poz_data.get("PozID"),
        }
    )

    item.save(ignore_permissions=True)
    logger.info(f"{'Updated' if item.get('name') else 'Created'} item {item_code}")
    return item


def get_tax_account():
    """Get or create tax account"""
    account_filters = {
        "account_name": DEFAULT_TAX_ACCOUNT["name"],
        "account_number": DEFAULT_TAX_ACCOUNT["number"],
    }

    if not frappe.db.exists("Account", account_filters):
        company = frappe.get_doc("Company", frappe.defaults.get_user_default("company"))
        account = frappe.new_doc("Account")
        account.update(
            {
                "account_name": DEFAULT_TAX_ACCOUNT["name"],
                "account_number": DEFAULT_TAX_ACCOUNT["number"],
                "parent_account": f"391 - HESAPLANAN KDV - {company.abbr}",
                "currency": "TRY",
                "account_type": "Tax",
                "tax_rate": DEFAULT_TAX_ACCOUNT["tax_rate"],
            }
        )
        account.save(ignore_permissions=True)
        return account

    return frappe.get_doc("Account", account_filters)
