import re

import frappe
from frappe import _

from ozerpan_ercom_sync.custom_api.utils import generate_logger, show_progress
from ozerpan_ercom_sync.utils import get_mysql_connection


@frappe.whitelist()
def sync_ercom():
    logger = generate_logger("sync_ercom")["logger"]
    sync_users(logger)
    sync_orders(logger)


def sync_users(logger) -> dict[str, str]:
    """
    Synchronizes user data from MySQL database to Frappe.

    Connects to MySQL database, retrieves all records from dbcari table,
    and creates corresponding Customer, Address and Contact records in Frappe.

    Returns:
        dict: Message indicating sync status
    """
    logger.info("Starting user sync")

    with get_mysql_connection() as connection:
        with connection.cursor() as cursor:
            query: str = "SELECT * FROM dbcari"
            cursor.execute(query)
            data: list[dict] = cursor.fetchall()

            if not data:
                logger.warning("No user data found")
                return {"message": "No data found"}

            create_users(data, logger)

    logger.info("User sync completed")
    return {"message": "Sync Completed"}


def create_users(data: list[dict], logger) -> None:
    """
    Creates Customer, Address and Contact records from imported data.

    Args:
        data_list (list[dict]): List of dictionaries containing user data from MySQL
    """
    data_len = len(data)
    for i, row in enumerate(data):
        show_progress(
            curr_count=i + 1,
            max_count=data_len,
            title="Customer",
            desc=f"Updating Customers... {i+1}/{data_len}",
        )
        if frappe.db.exists("Customer", {"customer_name": row["ADI"]}):
            logger.info(f"Customer already exists: {row['ADI']}")
            continue

        logger.info(f"Creating new customer: {row['ADI']}")
        customer_result: dict[str, str] = create_customer(row, logger)
        address_result: dict[str, str] = create_address(
            row, customer_result["docname"], logger
        )
        contact_result: dict[str, str] = create_contact(
            row, customer_result["docname"], address_result["docname"], logger
        )
        customer = frappe.get_doc("Customer", customer_result["docname"])
        customer.customer_primary_address = address_result["docname"]
        customer.customer_primary_contact = contact_result["docname"]
        customer.save()


def create_customer(data: dict, logger) -> dict[str, str]:
    """
    Creates a new Customer record in Frappe.

    Args:
        data (dict): Dictionary containing customer data

    Returns:
        dict: Message and docname of created Customer
    """
    c = frappe.new_doc("Customer")
    c.customer_name = str(data["ADI"])
    c.customer_type = "Company"
    if data.get("GRUP"):
        c.custom_group_for_ercom_db = str(data["GRUP"])
    c.custom_current_code = str(data["KOD"])
    c.customer_details = str(data["NOTLAR"])
    c.custom_tax_office = str(data["VDAIRESI"])
    c.tax_id = str(data["VERGINO"])
    c.insert(ignore_permissions=True)
    logger.info(f"Created customer {c.customer_name}")
    return {"msg": "Customer created successfully.", "docname": c.name}


def create_address(data: dict, customer: str, logger) -> dict[str, str]:
    """
    Creates a new Address record in Frappe.

    Args:
        data (dict): Dictionary containing address data
        customer (str): Customer docname to link address to

    Returns:
        dict: Message and docname of created Address
    """
    a = frappe.new_doc("Address")
    a.address_title = str(data["ADI"])
    a.address_type = "Billing"
    a.address_line1 = str(data["ADRES1"] or data["ADI"])
    a.address_line2 = str(data["ADRES2"])
    a.city = str(data["SEHIR"] or "Bilinmiyor")
    a.country = "Turkey" if frappe.db.exists("Country", "Turkey") else "Türkiye"
    a.pincode = str(data["POSTAKODU"])
    if data.get("EMAIL"):
        a.email_id = str(data.get("EMAIL"))
    a.phone = str(data["TELEFON1"])
    a.fax = str(data["FAKS"])
    a.append("links", {"link_doctype": "Customer", "link_name": customer})
    a.insert(ignore_permissions=True)
    logger.info(f"Created address for customer {customer}")
    return {"msg": "Address created successfully.", "docname": a.name}


def create_contact(data: dict, customer: str, address: str, logger) -> dict[str, str]:
    """
    Creates a new Contact record in Frappe.

    Args:
        data (dict): Dictionary containing contact data
        customer (str): Customer docname to link contact to
        address (str): Address docname to link contact to

    Returns:
        dict: Message and docname of created Contact
    """
    c = frappe.new_doc("Contact")
    c.status = "Open"
    c.full_name = str(data["ADI"])
    c.address = address
    c.is_primary_contact = 1
    c.append("links", {"link_doctype": "Customer", "link_name": customer})
    if data["EMAIL"]:
        c.append("email_ids", {"email_id": str(data["EMAIL"]), "is_primary": 1})
    if is_valid_phone(data["TELEFON1"]):
        c.append("phone_nos", {"phone": str(data["TELEFON1"]), "is_primary_phone": 1})
    if is_valid_phone(data["TELEFON2"]):
        c.append("phone_nos", {"phone": str(data["TELEFON2"]), "is_primary_phone": 0})
    c.insert(ignore_permissions=True)
    logger.info(f"Created contact for customer {customer}")
    return {"msg": "Contact created successfully.", "docname": c.name}


def is_valid_phone(phone: str) -> bool:
    """
    Validates phone number format.

    Args:
        phone: Phone number to validate

    Returns:
        bool: True if phone number is valid, False otherwise
    """
    if (
        isinstance(phone, str)
        and phone.strip()
        and re.match(r"^\+?\d{7,15}$", phone.strip())
    ):
        return True
    return False


def sync_orders(logger):
    """Synchronizes orders from MySQL database to Frappe."""
    with get_mysql_connection() as connection:
        with connection.cursor() as cursor:
            LIMIT: int = 100
            # query: str = f"SELECT * FROM dbsiparis ORDER BY SAYAC DESC LIMIT {LIMIT}"
            query: str = "SELECT * FROM dbsiparis WHERE SIPARISNO = 'S404228'"
            cursor.execute(query)
            data = cursor.fetchall()

            if not data:
                logger.warning("No order data found")
                return {"message": "No data found."}

            logger.info("Starting order sync")
            placeholder_item = get_placeholder_item()
            data_len = len(data)

            for i, row in enumerate(data):
                show_progress(
                    curr_count=i + 1,
                    max_count=data_len,
                    title=_("Sales Order"),
                    desc=f"Updating Sales Order... {i+1}/{data_len}",
                )
                try:
                    create_sales_order(
                        data=row, placeholder_item=placeholder_item, logger=logger
                    )
                except Exception as e:
                    logger.error(f"Error creating sales order: {str(e)}")
                    continue

            logger.info("Order sync completed")
            return {"message": "Sync Completed"}


def create_sales_order(data: dict, placeholder_item: str, logger) -> None:
    """Creates a sales order in Frappe from imported data."""
    order_no = data.get("SIPARISNO")
    customer_name = data.get("CARIUNVAN")
    current_code = data.get("CARIKOD")

    if frappe.db.exists("Sales Order", {"custom_ercom_order_no": order_no}):
        logger.info(f"Sales Order {order_no} already exists")
        return

    # TODO: Get customer by carikod
    if not frappe.db.exists("Customer", {"custom_current_code": current_code}):
        error_msg = f"Customer ({customer_name}-{current_code}) does not exist for order ({order_no})"
        logger.error(error_msg)
        frappe.throw(_(error_msg))

    customer = frappe.get_doc("Customer", {"custom_current_code": current_code})

    so = frappe.new_doc("Sales Order")
    so_data = {
        "custom_ercom_order_no": order_no,
        "transaction_date": data.get("SIPTARIHI"),
        "delivery_date": data.get("SEVKTARIHI"),
        "customer": customer.get("name"),
        "custom_remarks": data.get("NOTLAR"),
        "company": frappe.defaults.get_user_default("company"),
        "order_type": "Sales",
        "currency": "TRY",
        "selling_price_list": "Standard Selling",
        "apply_discount_on": "Grand Total",
        "additional_discount_percentage": customer.get("custom_total_discount_rate"),
    }

    so.update(so_data)
    so.append(
        "items",
        {
            "item_code": placeholder_item,
            "item_name": placeholder_item,
            "delivery_date": data.get("SEVKTARIHI"),
            "qty": 1,
            "uom": "Nos",
        },
    )
    so.save(ignore_permissions=True)
    logger.info(f"Created sales order {order_no}")


def get_placeholder_item() -> str:
    """Gets or creates a placeholder item for sales orders."""
    placeholder_item_name = "Place Holder Item"
    if not frappe.db.exists("Item", placeholder_item_name):
        placeholder_item = frappe.get_doc(
            {
                "doctype": "Item",
                "item_code": placeholder_item_name,
                "item_name": placeholder_item_name,
                "item_group": "All Item Groups",
                "stock_uom": "Nos",
            }
        )
        placeholder_item.insert()
    return placeholder_item_name
