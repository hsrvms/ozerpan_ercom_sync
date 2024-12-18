frappe.ui.form.on("Customer", {
  onload(frm) {
    console.log("--Onload--");
  },
  refresh(frm) {
    console.log("--Refresh--");
  },
});

frappe.ui.form.on("Ozerpan Discount", {
  custom_ozerpan_discount_add(frm) {
    console.log("--Add Row--");
  },
  custom_ozerpan_discount_remove(frm) {
    console.log("--Remove Row--");
    calculate_total_discount(frm);
  },
  rate(frm) {
    console.log("--Rate--");
    calculate_total_discount(frm);
  },
});

function calculate_total_discount(frm) {
  const discounts = frm.doc.custom_ozerpan_discount;
  remaining_factor = 1.0;
  for (d of discounts) {
    remaining_factor *= 1 - d.rate / 100;
  }
  total_discount_rate = (1 - remaining_factor) * 100;
  frm.set_value("custom_total_discount_rate", total_discount_rate);
}
