import logging

import frappe
import pandas as pd
from frappe import _

from ozerpan_ercom_sync.custom_api.utils import (
    check_file_type,
    generate_logger,
    get_file_info,
    get_float_value,
    show_progress,
)


@frappe.whitelist()
def update_opt_genel_dst_list(file_url: str) -> dict[str, str]:
    logger_dict = generate_logger("opt_genel_update")
    logger = logger_dict["logger"]
    log_file = logger_dict["log_file"]

    try:
        logger.info(f"Starting Opt Genel update process for file: {file_url}")
        file = get_file_info(file_url, logger)
        process_dst_excel_file(file, logger)
        logger.info("Opt Genel update with DST Excel file completed successfully.")

        return {
            "status": "Success",
            "message": _("DST Excel file processed successfully."),
            "log_file": log_file,
        }

    except (frappe.ValidationError, Exception) as e:
        logger.error(f"Error during Opt Genel update: {str(e)}")
        raise


def process_dst_excel_file(file: dict, logger: logging.Logger) -> None:
    check_file_type(file.get("extension"), "excel")
    logger.info(f"Processing Excel file: {file.get('name')}")

    if not frappe.db.exists("Opt Genel", {"opt_code": file.get("code")}):
        raise frappe.ValidationError(
            f"Opt Genel with code {file.get('code')} does not exist."
        )

    try:
        df = pd.read_excel(file.get("path"))

        if not isinstance(df, pd.DataFrame) or df.empty:
            raise ValueError("Invalid or empty Excel file")

        required_columns = {"STOK KODU", "AÇIKLAMA", "OLCU"}
        if not required_columns.issubset(df.columns):
            raise ValueError(
                f"Missing required columns: {required_columns - set(df.columns)}"
            )

        logger.info(f"Dataframe dimensions: {len(df)} rows, {len(df.columns)} columns")
        update_opt_dst(file.get("code"), df, logger)

    except Exception as e:
        logger.error(f"Error processing Excel file: {str(e)}")
        raise


def update_opt_dst(opt_code: str, df: pd.DataFrame, logger: logging.Logger) -> None:
    try:
        opt = frappe.get_doc("Opt Genel", {"opt_code": opt_code})
        items_data = []
        df_len = len(df)

        for idx, row in df.iterrows():
            show_progress(
                idx + 1,
                df_len,
                _("Opt Genel table sync."),
                _("Updating items {0} of {1}").format(idx + 1, df_len),
            )

            stock_code = str(row["STOK KODU"])
            item_code = frappe.db.exists("Item", f"erc-{stock_code}")

            if not item_code:
                error_msg = f"Item not found for stock code: {stock_code}"
                logger.error(error_msg)
                frappe.throw(error_msg)

            items_data.append(
                {
                    "item_code": item_code,
                    "item_name": str(row["AÇIKLAMA"]),
                    "size": get_float_value(str(row["OLCU"])),
                }
            )

        opt.set("dst_list", items_data)
        opt.save(ignore_permissions=True)
        logger.info(f"Successfully updated Opt Genel: {opt.name}")

    except Exception as e:
        logger.error(f"Error creating/updating Opt Genel doc: {str(e)}")
        raise
