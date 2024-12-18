frappe.ui.form.on("Sales Order", {
  onload(frm) {
    console.log("--Onload--");
  },
  refresh(frm) {
    console.log("--Refresh--");
  },
});

frappe.ui.form.on("Sales Order Item", {
  items_add(frm) {
    console.log("--Items Add--");
  },
  items_remove(frm) {
    console.log("--Items Remove--");
  },
  amount(frm) {
    console.log("--Amount--");
  },
  item_code(frm) {
    console.log("--Item Code--");
  },
  rate(frm) {
    console.log("--Rate--");
    apply_discount(frm);
  },
});

async function apply_discount(frm) {
  console.log("--Apply Discount--");
  const customer = await frappe.db.get_doc("Customer", frm.doc.customer);
  total_discount = customer.custom_total_discount_rate;
  frm.set_value("apply_discount_on", "Grand Total");
  frm.set_value("additional_discount_percentage", total_discount);
}
