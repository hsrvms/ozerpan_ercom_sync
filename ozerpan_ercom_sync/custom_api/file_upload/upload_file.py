import logging
from typing import Dict

import frappe
from frappe import _

from ozerpan_ercom_sync.custom_api.file_upload.mly_file_upload import (
    process_mly_file,
)
from ozerpan_ercom_sync.custom_api.file_upload.opt_file_upload import process_opt_file
from ozerpan_ercom_sync.custom_api.utils import (
    generate_logger,
    get_file_info,
)

ALLOWED_EXTENSIONS = {".xls", ".xlsx"}


@frappe.whitelist()
def upload_file(file_url: str) -> Dict[str, str]:
    """Upload and process Excel file"""
    logger_dict = generate_logger("file_upload")
    logger = logger_dict["logger"]
    log_file = logger_dict["log_file"]

    try:
        logger.info(f"Defining file: {file_url}")
        file = get_file_info(file_url, logger)
        validate_file(file)
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
        raise


def validate_file(file: Dict) -> None:
    """Validate file extension and path"""
    file_extension = file.get("extension", "").lower()
    file_path = file.get("path", "")

    if not file_path:
        raise frappe.ValidationError("File path not found.")

    if file_extension not in ALLOWED_EXTENSIONS:
        raise frappe.ValidationError(
            f"Invalid file format. Allowed formats: {', '.join(ALLOWED_EXTENSIONS)}"
        )


def process_file_by_category(file: Dict, logger: logging.Logger) -> None:
    """Route file processing based on category"""
    logger.info("Detecting file category...")
    file_category = file.get("category", "").lower()
    if file_category.startswith("mly"):
        logger.info("MLY file detected.")
        process_mly_file(file, logger)
    if file_category.startswith("opt"):
        logger.info("OPT file detected.")
        process_opt_file(file, logger)
