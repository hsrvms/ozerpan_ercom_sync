frappe.require([], () => {
  frappe.ui.form.on("Customer", {
    refresh(frm) {
      console.log("-- form refresh --");
    },
    onload(frm) {
      console.log("-- form Onload --");
    },
  });
});
