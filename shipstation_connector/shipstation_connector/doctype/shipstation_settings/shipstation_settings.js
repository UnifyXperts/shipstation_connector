frappe.ui.form.on("Shipstation Settings", {

    update_carriers(frm){
        
        frappe.call({
            method: "shipstation_connector.shipstation_connector.api.shipstation_connector.update_carriers",
            freeze: true,
            freeze_message: "Syncing Carriers...",
            callback: function (r) {

    if (!r.exc && r.message) {

        let carriers = r.message.carriers;

        frm.clear_table("carriers");

        carriers.forEach(c => {

            if (c.services && c.services.length > 0) {

                c.services.forEach(service => {

                    let row = frm.add_child("carriers");

                    row.carrier = c.friendly_name;
                    row.carrier_id = c.carrier_id;
                    row.carrier_code = c.carrier_code;
                    row.service_code = service.service_code;
                    row.is_active = 1;
                    row.is_default = c.primary ? 1 : 0;

                });

            }

        });

        frm.refresh_field("carriers");
    }
}
        });

    }

});