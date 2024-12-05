import re

# import phpserialize
import frappe
from ozerpan_ercom_sync.utils import get_mysql_connection


@frappe.whitelist()
def sync_items() -> dict[str, str]:
    """
    Synchronizes items from MySQL database to Frappe.

    Connects to MySQL database, retrieves records from dbpoz table ordered by PozID desc
    with a limit of 2 records, and creates corresponding Item and BOM records in Frappe
    if they don't already exist.

    Returns:
        dict: Message indicating sync status
    """
    LIMIT: int = 20
    connection = get_mysql_connection()
    cursor = connection.cursor(dictionary=True)
    query: str = f"SELECT * FROM dbpoz ORDER BY PozID DESC LIMIT {LIMIT}"
    cursor.execute(query)
    data = cursor.fetchall()

    if not data:
        return {"message": "No data found."}

    for row in data:
        item_code: str = f"{row.get('SIPARISNO')}-{row.get('POZNO')}"
        if frappe.db.exists("Item", {"item_code": item_code}):
            print(f"Item already exist: {item_code}")
            continue
        item_result: dict[str, str] = create_item(row)
        _ = create_bom(row, item_result["docname"])

    cursor.close()
    connection.close()
    return {"message": "Sync Completed"}


def create_item(row: dict) -> dict[str, str]:
    """
    Creates a new Item record in Frappe from MySQL row data.

    Args:
        row (dict): Dictionary containing item data from MySQL dbpoz table

    Returns:
        dict: Message indicating success and docname of created Item
            e.g. {"msg": "Item created successfully.", "docname": "ITEM-001"}
    """
    i = frappe.new_doc("Item")
    custom_code: str = f"{row.get('SIPARISNO')}-{row.get('POZNO')}"
    i.item_code = custom_code
    i.item_name = custom_code
    i.item_group = "Products"
    i.stock_uom = "Meter"
    i.valuation_rate = row.get("TUTAR")
    i.description = row.get("ACIKLAMA")
    i.custom_serial = row.get("SERI")
    i.custom_width = row.get("GENISLIK")
    i.custom_height = row.get("HEIGHT")
    i.custom_color = row.get("RENK")
    i.custom_quantity = row.get("ADET")
    i.custom_remarks = row.get("NOTLAR")
    i.custom_poz_id = row.get("PozID")
    i.insert(ignore_permissions=True)
    print(f"item code: {i.item_code}")
    print(f"docname: {i.name}")
    return {"msg": "Item created successfully.", "docname": i.name}


def create_bom(row: dict, item: str) -> dict[str, str]:
    """
    Creates a new Bill of Materials (BOM) record in Frappe from MySQL row data.

    Args:
        row (dict): Dictionary containing BOM data from MySQL dbpoz table
        item (str): Item docname to link BOM to

    Returns:
        dict: Message indicating success and docname of created BOM
            e.g. {"msg": "BOM created successfully.", "docname": "BOM-001"}
    """
    company: str = frappe.defaults.get_user_default("Company")
    b = frappe.new_doc("BOM")
    b.item = item
    b.company = company
    b.quantity = row.get("ADET")
    b.append(
        "items",
        {
            "item_code": item,
        },
    )
    b.insert(ignore_permissions=True)
    b.submit()
    print(f"BOM ITEM: {b.item}")
    print(f"BOM docname: {b.name}")
    return {"msg": "BOM created successfully.", "docname": b.name}


########################## SYNC USERS #########################
@frappe.whitelist()
def sync_users() -> dict[str, str]:
    """
    Synchronizes user data from MySQL database to Frappe.

    Connects to MySQL database, retrieves all records from dbcari table,
    and creates corresponding Customer, Address and Contact records in Frappe.

    Returns:
        dict: Message indicating sync status
    """
    connection = get_mysql_connection()
    cursor = connection.cursor(dictionary=True)
    query: str = "SELECT * FROM dbcari"
    cursor.execute(query)
    data: list[dict] = cursor.fetchall()

    if not data:
        return {"message": "No data found"}

    create_users(data)
    # frappe.throw("Some Error")

    cursor.close()
    connection.close()
    return {"message": "Sync Completed"}


def create_users(data: list[dict]) -> None:
    """
    Creates Customer, Address and Contact records from imported data.

    Args:
        data_list (list[dict]): List of dictionaries containing user data from MySQL
    """
    for row in data:
        if frappe.db.exists("Customer", {"customer_name": row["ADI"]}):
            print(f"Customer already exist: {row['ADI']}")
            continue

        customer_result: dict[str, str] = create_customer(row)
        address_result: dict[str, str] = create_address(row, customer_result["docname"])
        contact_result: dict[str, str] = create_contact(
            row, customer_result["docname"], address_result["docname"]
        )
        customer = frappe.get_doc("Customer", customer_result["docname"])
        customer.customer_primary_address = address_result["docname"]
        customer.customer_primary_contact = contact_result["docname"]
        customer.save()


def create_customer(data: dict) -> dict[str, str]:
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
    c.custom_group_for_ercom_db = str(data["GRUP"])
    c.custom_current_code = str(data["KOD"])
    c.customer_details = str(data["NOTLAR"])
    c.custom_tax_office = str(data["VDAIRESI"])
    c.tax_id = str(data["VERGINO"])
    c.insert(ignore_permissions=True)
    return {"msg": "Customer created successfully.", "docname": c.name}


def create_address(data: dict, customer: str) -> dict[str, str]:
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
    a.country = "Turkey"
    a.pincode = str(data["POSTAKODU"])
    a.email_id = str(data["EMAIL"])
    a.phone = str(data["TELEFON1"])
    a.fax = str(data["FAKS"])
    a.append("links", {"link_doctype": "Customer", "link_name": customer})
    a.insert(ignore_permissions=True)
    return {"msg": "Address created successfully.", "docname": a.name}


def create_contact(data: dict, customer: str, address: str) -> dict[str, str]:
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
