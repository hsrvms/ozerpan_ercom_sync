frappe.listview_settings["TesDetay"] = {
  refresh(listview) {
    console.log("-- ListView Refresh --");
  },
  onload(listview) {
    console.log("-- ListView Onload --");
    syncTesDetayBtn(listview);
  },
};

function syncTesDetayBtn(listview) {
  listview.page.add_inner_button(__("Sync TesDetay"), () =>
    callSyncTesDetayApi(),
  );
}

function callSyncTesDetayApi() {
  frappe.call({
    method: "ozerpan_ercom_sync.custom_api.tes_detay.sync_tes_detay",
    freeze: true,
    callback: (r) => {
      if (r.message) {
        console.log(r.message);
        frappe.msgprint({
          title: __("Success"),
          indicator: "green",
          message: __("TesDetay synchronized successfully."),
        });
      }
    },
    error: (r) => {
      console.log(r.message);
      frappe.msgprint({
        title: __("Error"),
        indicator: "red",
        message: __("An error occurred while synchronizing TesDetay."),
      });
    },
  });
}
