import logging

import frappe
import pandas as pd
from frappe import _

from ozerpan_ercom_sync.custom_api.utils import (
    generate_logger,
    get_file_info,
    show_progress,
)
from ozerpan_ercom_sync.utils import get_mysql_connection


@frappe.whitelist()
def upload_file(file_url: str) -> dict[str, str]:
    logger_dict = generate_logger("file_upload")
    logger = logger_dict["logger"]
    log_file = logger_dict["log_file"]

    ALLOWED_EXTENSIONS = {".xls", ".xlsx"}

    try:
        logger.info(f"Defining file: {file_url}")
        file = get_file_info(file_url, logger)
        file_extension = file.get("extension", "").lower()
        file_path = file.get("path", "")

        if not file_path:
            raise frappe.ValidationError("File path not found.")

        if file_extension not in ALLOWED_EXTENSIONS:
            raise frappe.ValidationError(
                f"Invalid file format. Allowed formats: {', '.join(ALLOWED_EXTENSIONS)}"
            )

        process_file_by_category(file, logger)

        return {
            "status": "Success",
            "message": _("File processed successfully."),
            "log_file": log_file,
        }

    except frappe.ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error during File Upload: {str(e)}")


def process_file_by_category(file: dict, logger: logging.Logger):
    logger.info("Detecting file category...")
    file_category = file.get("category").lower()
    if file_category.startswith("mly"):
        logger.info("MLY file detected.")
        process_mly(file, logger)


def process_mly(file: dict, logger: logging.Logger):
    logger.info("Processing MLY file...")
    file_path = file.get("path")
    try:
        try:
            df = pd.read_excel(file_path)
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {str(e)}")

        if not isinstance(df, pd.DataFrame):
            raise ValueError("Invalid Excel file format.")

        if df.empty:
            raise ValueError("Excel file is empty.")

        ef = pd.ExcelFile(file_path)
        sheets = ef.sheet_names

        order_no = pd.read_excel(file_path).tail(3)["Stok Kodu"].iloc[0]
        poz_data = get_poz_data(order_no, logger)
        print("\n\n\n")
        print("Sheets:", sheets)
        print("Order No:", order_no)
        print(f"poz_data: {poz_data[0].get('ADET')}")
        print("\n\n\n")
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
        so = frappe.get_last_doc(
            "Sales Order",
            {
                "custom_ercom_order_no": order_no,
                "status": "Draft",
            },
        )

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
        so_item_table = []

        for i, sheet in enumerate(sheets):
            show_progress(
                curr_count=i + 1,
                max_count=sheets_len,
                title=_("Sales Order File Upload"),
                desc=_("Syncing Sheet {0} of {1}").format(i + 1, sheets_len),
            )
            logger.info(f"Processing sheet: {sheet}")

            df = pd.read_excel(file_path, sheet, dtype=dtype_dict)
            if df.empty:
                logger.warning(f"Empty sheet found: {sheet}")
                continue

            tail = df.tail(3).copy()
            filtered_df = df[df["Stok Kodu"].str.startswith("#", na=False)].copy()
            item_code = f"{tail['Stok Kodu'].iloc[0]}-{tail['Stok Kodu'].iloc[1]}"
            total_price = tail["Toplam Fiyat"].iloc[0]
            logger.info(f"Processing item: {item_code}")

            item = create_item(
                item_code=item_code,
                total_price=total_price,
                poz_data=poz_data[i],
                logger=logger,
            )
            print("ItemItem:", item)
            bom_result = create_bom(
                item_name=item.name,
                qty=poz_data[i].get("ADET"),
                df=filtered_df,
                logger=logger,
            )
            so_item_table.append(
                {
                    "item_code": item.item_code,
                    "item_name": item.item_name,
                    "description": item.description,
                    "qty": item.custom_quantity,
                    "uom": item.stock_uom,
                }
            )
        print("so items table:", so_item_table)
        so.set("items", so_item_table)
        so.save(ignore_permissions=True)
        # so.submit()

    except Exception as e:
        logger.error(f"Error processing MLY file: {str(e)}")
        frappe.throw(_("Error processing MLY file: {0}").format(str(e)))


def get_poz_data(order_no: str, logger: logging.Logger) -> list[tuple]:
    """Get order data from dbpoz table

    Args:
        order_no: The order number to query for
        logger: Logger instance for logging messages

    Returns:
        List of tuples containing order data rows

    Raises:
        frappe.ValidationError: If there is an error fetching the data
    """
    try:
        with get_mysql_connection() as connection:
            with connection.cursor() as cursor:
                query = "SELECT SAYAC, SIPARISNO, GENISLIK, YUKSEKLIK, ADET, SERI, ACIKLAMA, NOTLAR, PozID FROM dbpoz WHERE SIPARISNO = %s"
                cursor.execute(query, (order_no,))
                results = cursor.fetchall()
                logger.info(f"Retrieved {len(results)} records for order {order_no}")
                return results
    except Exception as e:
        error_msg = f"Error fetching data for order {order_no}: {str(e)}"
        logger.error(error_msg)
        frappe.log_error(error_msg)
        raise frappe.ValidationError(error_msg)


def create_bom(item_name: str, qty, df, logger: logging.Logger):
    company: str = frappe.defaults.get_user_default("Company")
    b = frappe.new_doc("BOM")
    b.item = item_name
    b.company = company
    b.quantity = qty
    items_table = []

    for idx, row in df.iterrows():
        print(f"\n\n\nROW: {row}\n\n\n")
        stock_code = row["Stok Kodu"].lstrip("#")
        if not frappe.db.exists("Item", stock_code):
            raise ValueError(f"No such item: {stock_code}")
        item = frappe.get_doc("Item", stock_code)
        if not item.custom_kit:
            rate = get_float_value(str(row.get("Birim Fiyat", "0.0")))
            amount = get_float_value(str(row.get("Toplam Fiyat", "0.0")))
            item_qty = (
                round((amount / rate), 7)
                if rate != 0.0
                else get_float_value(row.get("Miktar"))
            )
            items_table.append(
                {
                    "item_code": item.get("item_code"),
                    "item_name": item.get("item_name"),
                    "description": item.get("description"),
                    "uom": str(row.get("Birim")),
                    "qty": item_qty,
                    "rate": rate,
                }
            )
    print("Table:", items_table)
    b.set("items", items_table)
    b.save(ignore_permissions=True)
    b.submit()
    logger.info(f"Created BOM for item {item_name}")
    return {"msg": "BOM created successfully.", "docname": b.name}


def create_item(
    item_code: str, total_price: float, poz_data: dict, logger: logging.Logger
):
    if frappe.db.exists("Item", {"item_code": item_code}):
        i = frappe.get_doc("Item", {"item_code": item_code})
        print("--Get Item--")
    else:
        i = frappe.new_doc("Item")
        print("--Create Item--")
    i.item_code = item_code
    i.item_name = item_code
    i.item_group = "All Item Groups"
    i.stock_uom = "Nos"
    i.valuation_rate = total_price
    i.description = poz_data.get("ACIKLAMA")
    i.custom_serial = poz_data.get("SERI")
    i.custom_width = poz_data.get("GENISLIK")
    i.custom_height = poz_data.get("YUKSEKLIK")
    i.custom_color = poz_data.get("RENK")
    i.custom_quantity = poz_data.get("ADET")
    i.custom_remarks = poz_data.get("NOTLAR")
    i.custom_poz_id = poz_data.get("PozID")
    i.save(ignore_permissions=True)
    logger.info(f"Created item {item_code}")
    return i


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


def get_float_value(value: str) -> float:
    cleaned_value = (
        value.lower().replace("tl", "").strip().replace(".", "").replace(",", ".")
    )
    return float(cleaned_value)
