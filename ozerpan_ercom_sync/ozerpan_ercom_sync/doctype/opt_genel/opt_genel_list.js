frappe.listview_settings["Opt Genel"] = {
  refresh(listview) {
    console.log("-- ListView Refresh --");
  },
  onload(listview) {
    console.log("-- ListView Onload --");
    uploadOptiGenelListBtn(listview);
    uploadDSTListBtn(listview);
  },
};

function uploadOptiGenelListBtn(listview) {
  listview.page.add_inner_button(__("Upload Opt Genel"), () => {
    let d = new frappe.ui.Dialog({
      title: __("Select Excel File"),
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
        callUploadOptGenelApi(values);
        d.hide();
      },
    });
    d.show();
  });
}

function callUploadOptGenelApi(values) {
  frappe.call({
    method: "ozerpan_ercom_sync.custom_api.file_upload.upload_file.upload_file",
    args: {
      file_url: values.file,
    },
    freeze: true,
    callback: (r) => {
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
      frappe.msgprint({
        title: __("Error"),
        indicator: "red",
        message: __("An error occurred while processing the file."),
      });
    },
  });
}

// |#|#|#|#|#|#|

function uploadDSTListBtn(listview) {
  listview.page.add_inner_button(__("Upload DST"), () => {
    let d = new frappe.ui.Dialog({
      title: __("Select Excel File"),
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
        callUploadDSTApi(values);
        d.hide();
      },
    });
    d.show();
  });
}

function callUploadDSTApi(values) {
  console.log("UPLOAD DST API");
  frappe.call({
    method: "ozerpan_ercom_sync.custom_api.dst.update_opt_genel_dst_list",
    args: {
      file_url: values.file,
    },
    freeze: true,
    callback: (r) => {
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
      frappe.msgprint({
        title: __("Error"),
        indicator: "red",
        message: __("An error occurred while processing the file."),
      });
    },
  });
}
