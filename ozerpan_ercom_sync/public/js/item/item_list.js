frappe.listview_settings["Item"] = {
  refresh(listview) {
    console.log("-- ListView refresh --");
  },
  onload(listview) {
    console.log("-- ListView Onload --");
    addSyncCustomersBtn(listview);
  },
};

function addSyncCustomersBtn(listview) {
  listview.page.add_inner_button(__("Sync Items"), () => {
    frappe.call({
      method: "ozerpan_ercom_sync.api.sync_items",
      callback: function (r) {
        if (r.message) {
          console.log(r.message);
          frappe.msgprint(r.message);
        }
      },
    });
  });
}
