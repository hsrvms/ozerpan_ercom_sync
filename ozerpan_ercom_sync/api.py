import re

import frappe
from ozerpan_ercom_sync.utils import get_mysql_connection


@frappe.whitelist()
def sync_users():
    connection = get_mysql_connection()
    cursor = connection.cursor(dictionary=True)

    query = "SELECT * FROM dbcari"
    cursor.execute(query)
    data = cursor.fetchall()

    if not data:
        return {"message": "No Data Found"}

    create_users(data)
    # frappe.throw("Some Error")

    cursor.close()
    connection.close()
    return {"message": "Sync Completed"}


def create_users(data_list):
    for key, value in data_list[8].items():
        print(f"{key}: {value}")

    print("#########")

    for data in data_list:
        if frappe.db.exists("Customer", {"customer_name": data["ADI"]}):
            print(f"Customer already exist: {data['ADI']}")
            continue

        customer_result = create_customer(data)
        address_result = create_address(data, customer_result["docname"])
        contact_result = create_contact(
            data, customer_result["docname"], address_result["docname"]
        )
        customer = frappe.get_doc("Customer", customer_result["docname"])
        customer.customer_primary_address = address_result["docname"]
        customer.customer_primary_contact = contact_result["docname"]
        customer.save()


def create_customer(data):
    c = frappe.new_doc("Customer")
    c.customer_name = data["ADI"]
    c.customer_type = "Company"
    c.custom_group_for_ercom_db = data["GRUP"]
    c.custom_current_code = data["KOD"]
    c.customer_details = data["NOTLAR"]
    c.custom_tax_office = data["VDAIRESI"]
    c.tax_id = data["VERGINO"]
    c.insert(ignore_permissions=True)
    return {"msg": "Customer created successfully.", "docname": c.name}


def create_address(data, customer):
    a = frappe.new_doc("Address")
    a.address_title = data["ADI"]
    a.address_type = "Billing"
    a.address_line1 = data["ADRES1"] or data["ADI"]
    a.address_line2 = data["ADRES2"]
    a.city = data["SEHIR"] or "Bilinmiyor"
    a.country = "Turkey"
    a.pincode = data["POSTAKODU"]
    a.email_id = data["EMAIL"]
    a.phone = data["TELEFON1"]
    a.fax = data["FAKS"]
    a.append("links", {"link_doctype": "Customer", "link_name": customer})
    a.insert(ignore_permissions=True)
    return {"msg": "Address created successfully.", "docname": a.name}


def create_contact(data, customer, address):
    c = frappe.new_doc("Contact")
    c.status = "Open"
    c.full_name = data["ADI"]
    c.address = address
    c.is_primary_contact = 1
    c.append("links", {"link_doctype": "Customer", "link_name": customer})
    if data["EMAIL"]:
        c.append("email_ids", {"email_id": data["EMAIL"], "is_primary": 1})
    if is_valid_phone(data["TELEFON1"]):
        c.append("phone_nos", {"phone": data["TELEFON1"], "is_primary_phone": 1})
    if is_valid_phone(data["TELEFON2"]):
        c.append("phone_nos", {"phone": data["TELEFON2"], "is_primary_phone": 0})
    c.insert(ignore_permissions=True)
    return {"msg": "Contact created successfully.", "docname": c.name}


def is_valid_phone(phone):
    if (
        isinstance(phone, str)
        and phone.strip()
        and re.match(r"^\+?\d{7,15}$", phone.strip())
    ):
        return True
    return False
