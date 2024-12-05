frappe.listview_settings["Customer"] = {
  refresh(listview) {
    console.log("-- ListView refresh --");
  },
  onload(listview) {
    console.log("-- ListView Onload --");
    addSyncCustomersBtn(listview);
  },
};

function addSyncCustomersBtn(listview) {
  listview.page.add_inner_button(__("Sync Customers"), () => {
    frappe.call({
      method: "ozerpan_ercom_sync.api.sync_users",
      callback: function (r) {
        if (r.message) {
          console.log(r.message);
          frappe.msgprint(r.message);
        }
      },
    });
  });
}
