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
    callSyncItemsApi();
  });
}

function callSyncItemsApi() {
  let loading = new frappe.ui.Dialog({
    title: __("Processing"),
    primary_action_label: __("Hide"),
    primary_action: () => loading.hide(),
  });

  loading.$body.html(`
    <div class="progress">
      <div class="progress-bar progress-bar-striped active" role="progressbar" style="width: 100%">
        <span>${__("Processing Item Sync... Please wait.")}</span>
      </div>
    </div>
    <div class="margin-top">
      <p class="text-muted">${__("This may take a few minutes. Please do not close this window.")}</p>
    </div>
    `);

  loading.show();

  frappe.call({
    method: "ozerpan_ercom_sync.api.sync_items",
    freeze: true,
    callback: (r) => {
      loading.hide();

      if (r.message) {
        console.log(r.message);
        frappe.msgprint({
          title: __("Success"),
          indicator: "green",
          message: __("Items synchronized successfully."),
        });
      }
    },
    error: (r) => {
      loading.hide();
      frappe.msgprint({
        title: __("Error"),
        indicator: "red",
        message: __("An error occurred while synchronizing the items."),
      });
    },
  });
}
