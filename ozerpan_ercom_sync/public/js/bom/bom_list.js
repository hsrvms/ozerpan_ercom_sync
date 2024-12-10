frappe.listview_settings["BOM"] = {
  refresh(listview) {
    console.log("-- ListView refresh --");
  },
  onload(listview) {
    console.log("-- ListView Onload --");
    addUploadFileBtn(listview);
  },
};

function addUploadFileBtn(listview) {
  listview.page.add_inner_button(__("Upload File"), () => {
    let d = new frappe.ui.Dialog({
      title: __("Select .xls File"),
      fields: [
        {
          label: "File",
          fieldname: "file",
          fieldtype: "Attach",
          reqd: 1,
        },
      ],
      size: "small",
      primary_action_label: __("Submit"),
      primary_action(values) {
        console.log(values);
        callUploadBomApi(values);
        d.hide();
      },
    });
    d.show();
  });
}

function callUploadBomApi(values) {
  let loading = new frappe.ui.Dialog({
    title: __("Processing"),
    primary_action_label: __("Hide"),
    primary_action: () => loading.hide(),
  });

  loading.$body.html(`
    <div class="progress">
      <div class="progress-bar progress-bar-striped active" role="progressbar" style="width: 100%">
        <span>${__("Processing BOM update... Please wait.")}</span>
      </div>
    </div>
    <div class="margin-top">
      <p class="text-muted">${__("This may take a few minutes. Please do not close this window.")}</p>
    </div>
    `);

  loading.show();

  frappe.call({
    method: "ozerpan_ercom_sync.api.update_bom",
    args: {
      file_url: values.file,
    },
    freeze: true,
    callback: (r) => {
      loading.hide();

      if (r.message) {
        console.log(r.message);
        frappe.msgprint({
          title: __("Success"),
          indicator: "green",
          message: __("File processed successfully."),
        });
      }
    },
    error: (r) => {
      loading.hide();
      frappe.msgprint({
        title: __("Error"),
        indicator: "red",
        message: __("An error occurred while processing the file."),
      });
    },
  });
}
