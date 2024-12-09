import os
import re

import frappe
import pandas as pd

from ozerpan_ercom_sync.utils import get_mysql_connection


@frappe.whitelist()
def update_bom(file_url):
    file_doc = frappe.get_doc("File", {"file_url": file_url})
    print(f"\n\n\nFile:{file_doc}\n\n\n")

    file_path = frappe.get_site_path(
        "private" if file_doc.is_private else "public",
        "files",
        os.path.basename(file_url),
    )

    if not os.path.exists(file_path):
        frappe.throw(f"File not found: {file_path}")

    data = process_excel(file_path)

    return {"status": "Success", "message": ""}


def process_excel(file_path):
    ef = pd.ExcelFile(file_path)
    sheets = ef.sheet_names
    for sheet in sheets:
        df = pd.read_excel(file_path, sheet)
        if df.empty:
            print(f"Sheet '{sheet}' is empty. Skipping...")
            continue
        tail = df.tail(3)
        item_code = f"{tail['Stok Kodu'].values[0]}-{tail['Stok Kodu'].values[1]}"
        if not frappe.db.exists("BOM", {"item": item_code}):
            frappe.throw(f"BOM for the Item ({item_code}) does not exist.")
        bom_doc = frappe.get_doc("BOM", {"item": item_code})
        filtered_df = df[df["Stok Kodu"].astype(str).str.startswith("#")]
        update_bom_raw_materials(bom_doc, filtered_df)
        update_bom_item_valuation_rate(item_code, tail["Toplam Fiyat"].values[0])


def update_bom_item_valuation_rate(item_code, total_amount):
    if not frappe.db.exists("Item", {"item_code": item_code}):
        frappe.throw(f"Item ({item_code}) does not exist")
    item = frappe.get_doc("Item", {"item_code": item_code})
    print(f"Item: {item.name}")
    print(f"Valuation Rate Before: {item.valuation_rate}")
    item.valuation_rate = get_float_value(total_amount)
    # item.db_set("valuation_rate", total_amount)
    item.save(ignore_permissions=True)
    print(f"Valuation Rate After: {item.valuation_rate}\n")


def update_bom_raw_materials(doc: "frappe.Document", df: pd.DataFrame) -> None:
    bom_doc = None
    if doc.docstatus.is_submitted():
        doc.cancel()
        bom_doc = frappe.copy_doc(doc)
        bom_doc.amended_from = doc.name
    elif doc.docstatus.is_draft():
        bom_doc = doc

    bom_doc.set("items", [])
    for _, row in df.iterrows():
        stock_code: str = str(row["Stok Kodu"]).lstrip("#")
        item: dict = create_or_update_raw_material_item(stock_code, row)
        rate = float(item.get("valuation_rate", 0.0))
        amount = get_float_value(str(row.get("Toplam Fiyat", "0.0")))

        print(f"Amount: {amount} | Rate: {rate==0.0}")
        qty = round((amount / rate), 7) if rate != 0.0 else 0.0000000
        bom_doc.append(
            "items",
            {
                "item_code": str(item.get("item_code")),
                "item_name": str(item.get("item_name")),
                "description": str(item.get("description")),
                "uom": str(item.get("stock_uom")),
                "stock_uom": str(item.get("stock_uom")),
                "qty": qty,
                "rate": rate,
            },
        )
    bom_doc.save(ignore_permissions=True)
    bom_doc.submit()


def create_or_update_raw_material_item(
    stock_code: str, data: pd.Series
) -> "frappe.Document":
    """
    Creates a new raw material item in Frappe or updates existing one.

    Args:
        stock_code (str): Stock code for the item, will be prefixed with "erc-"
        data (pd.Series): Pandas Series containing item data from Excel sheet with columns:
            - Stok Kodu: Stock code starting with #
            - Açıklama: Item description in Turkish
            - Birim: Unit of measure (adet, mtül, m², kg, litre, kutu, tane)
            - Birim Fiyat: Unit price in TL (e.g. "10,50 TL")
            - Birim Kg.: Weight per unit as decimal (e.g. "1,5")

    Returns:
        frappe.Document: The created or updated Item document with fields:
            - item_code: "erc-" + stock code
            - item_name: Description from Excel
            - description: Same as item_name
            - item_group: "Raw Material"
            - stock_uom: Mapped unit (Adet, Mtul, Square Meter, Kilogram, etc)
            - valuation_rate: Price as float
            - weight_per_unit: Weight as float

    Example:
        >>> stock_code = "1234"
        >>> data = pd.Series({
        ...     "Stok Kodu": "#1234",
        ...     "Açıklama": "Test Ürün",
        ...     "Birim": "adet",
        ...     "Birim Fiyat": "10,50 TL",
        ...     "Birim Kg.": "1,5"
        ... })
        >>> item = create_or_update_raw_material_item(stock_code, data)
        >>> print(item.item_code)
        'erc-1234'
        >>> print(item.stock_uom)
        'Adet'
    """
    item_code = f"erc-{stock_code}"
    existing_item = frappe.db.exists("Item", {"item_code": item_code})
    if existing_item:
        item = frappe.get_doc("Item", existing_item)
    else:
        item = frappe.new_doc("Item")
    item.item_code = item_code
    item.item_name = str(data.get("Açıklama", ""))
    item.description = str(data.get("Açıklama", ""))
    item.item_group = "Raw Material"
    item.stock_uom = get_uom(str(data.get("Birim", "")))
    item.valuation_rate = get_float_value(str(data.get("Birim Fiyat", "0.0")))
    item.weight_per_unit = get_float_value(str(data.get("Birim Kg.", "0.0")))
    item.save(ignore_permissions=True)
    print(f"Raw Material Item created/updated: {item.name}")
    return item


def get_float_value(value: str) -> float:
    """
    Converts a Turkish currency/number string to a float value.

    Args:
        value (str): String value to convert, e.g. "10,50 TL" or "1.234,56"

    Returns:
        float: Converted numeric value

    Examples:
        >>> get_float_value("10,50 TL")
        10.50
        >>> get_float_value("1.234,56")
        1234.56
    """
    cleaned_value = value.lower().replace("tl", "").strip()
    cleaned_value = cleaned_value.replace(".", "").replace(",", ".")
    return float(cleaned_value)


def get_uom(unit: str) -> str:
    """
    Converts a unit string to the corresponding standardized UOM (Unit of Measure).

    Args:
        unit (str): The input unit string to convert

    Returns:
        str: The standardized UOM string. Default is "Unit" if no match found.
            "Mtul" for "mtül"
            "Nos" for "adet"
            "Square Meter" for "m²"
    """
    uom: str = "Other"
    match str(unit).lower():
        case "mtül":
            uom = "Mtul"
        case "adet":
            uom = "Adet"
        case "m²":
            uom = "Square Meter"
        case "kg":
            uom = "Kilogram"
        case "litre":
            uom = "Litre"
        case "kutu":
            uom = "Box"
        case "tane":
            uom = "Tane"
    exists = frappe.db.exists("UOM", uom)
    if exists:
        return uom
    else:
        new_uom = frappe.new_doc("UOM")
        new_uom.uom_name = uom
        new_uom.enabled = 1
        new_uom.save(ignore_permissions=True)
        print(f"New UOM created: {new_uom.name}")
        return new_uom.name


#########################################################
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
    LIMIT: int = 100
    connection = get_mysql_connection()
    cursor = connection.cursor()
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
    # b.submit()
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
    cursor = connection.cursor()
    query: str = "SELECT * FROM dbcari"
    cursor.execute(query)
    data: list[dict] = cursor.fetchall()

    if not data:
        return {"message": "No data found"}

    for key, value in data[0].items():
        print(f"{key}: {value}")
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
    if data.get("GRUP"):
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
    if data.get("EMAIL"):
        a.email_id = str(data.get("EMAIL"))
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
