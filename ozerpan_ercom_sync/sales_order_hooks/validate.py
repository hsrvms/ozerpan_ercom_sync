import frappe
from frappe import _


def validate(doc, method):
    customer = frappe.get_doc("Customer", doc.customer)
    discount_rate = customer.custom_total_discount_rate

    if discount_rate and not is_valid_discount(doc, discount_rate):
        frappe.throw(_("Customer Discount was not applied to the Sales Order"))


def is_valid_discount(doc, discount_rate):
    return (
        doc.apply_discount_on == "Grand Total"
        and doc.additional_discount_percentage == discount_rate
    )
