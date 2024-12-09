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
  frappe.call({
    method: "ozerpan_ercom_sync.api.update_bom",
    args: {
      file_url: values.file,
    },
    callback: (r) => {
      if (r.message) {
        console.log(r.message);
        frappe.msgprint(__("File processed successfully."));
      }
    },
  });
}
