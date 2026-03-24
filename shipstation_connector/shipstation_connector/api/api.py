import requests
import frappe


settings = frappe.get_single("Shipstation Settings")
BASE_URL = settings.shipstation_endpoint
API_KEY = settings.get_password("v2_api_key")
NOTIFY_SELLER = settings.notify_seller
update_tracking_info = settings.update_tracking_info_on_marketplace
ACCOUNT_MAP = settings.account_mapper

@frappe.whitelist()
def create_and_set_addressv2(order_id, access_token):

    headers = {
        "API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    request_url = f"{BASE_URL}/shipments/external_shipment_id/{order_id}"
    response = requests.get(request_url, headers=headers)

    if response.status_code != 200:
        frappe.throw(response.text)

    address = response.json()["ship_to"]

    country_code = address.get("country_code")
    country_name = frappe.get_all("Country", filters={"code": country_code.lower()})

    line2 = address.get("address_line2")
    line3 = address.get("address_line3")

    existing_address = frappe.get_all(
        "Address",
        filters={
            "address_line1": address.get("address_line1"),
            "pincode": address.get("postal_code"),
        },
        fields=["name"],
        limit=1
    )

    if existing_address:
        address_name = existing_address[0].name
    else:
        customer_address = frappe.new_doc("Address")
        customer_address.address_title = f"{address.get('name')}"
        customer_address.address_type = "Shipping"
        customer_address.address_line1 = address.get("address_line1")

        if line2 and line3:
            customer_address.address_line2 = f"{line2} {line3}"
        elif line2:
            customer_address.address_line2 = line2
        elif line3:
            customer_address.address_line2 = line3
        else:
            customer_address.address_line2 = ""

        customer_address.city = address.get("city_locality")
        customer_address.country = country_name[0].name if country_name else "United States"
        customer_address.state = address.get("state_province")
        customer_address.pincode = address.get("postal_code")
        customer_address.email_id = address.get("email")

        customer_address.append("links", {
            "link_doctype": "Customer",
            "link_name": address.get('name')
        })

        customer_address.insert(ignore_permissions=True)
        address_name = customer_address.name

    sales_orders = frappe.get_all(
        "Sales Order",
        filters={"custom_marketplace_order_id": order_id},
        fields=["name"]
    )

    if not sales_orders:
        frappe.throw("No Sales Order found for this Marketplace Order ID")

    sales_order_doc = frappe.get_doc("Sales Order", sales_orders[0].name)

    if (
        sales_order_doc.customer_address == address_name
        and sales_order_doc.shipping_address_name == address_name
    ):
        return "Already set, skipped"

    sales_order_doc.db_set({
        "customer_address": address_name,
        "shipping_address_name": address_name
    })

    sales_order_doc.notify_update()

    return "Address Synced Successfully"

@frappe.whitelist()
def sync_addresses(access_token):

    sales_orders = frappe.get_all(
        "Sales Order",
        filters={"custom_marketplace_order_id": ["!=", ""]},
        fields=["name", "custom_marketplace_order_id"]
    )

    for so in sales_orders:
        order_id = so.custom_marketplace_order_id
        try:
            create_and_set_addressv2(order_id, access_token)
        except Exception:
            frappe.log_error(frappe.get_traceback(), f"Failed for {order_id}")

    return "Addresses Synced Successfully"
        

@frappe.whitelist()
def sync_addresses_in_background(access_token):

    sales_orders = frappe.get_all(
        "Sales Order",
        filters={
            "custom_marketplace_order_id": ["!=", ""],
            "customer_address": ["is", "not set"]
        },
        fields=["name", "custom_marketplace_order_id"]
    )

    for so in sales_orders:

        frappe.enqueue(
            "shipstation_connector.shipstation_connector.api.api.create_and_set_addressv2",
            queue="short",
            timeout=600,
            enqueue_after_commit=True,
            job_name=f"sync_address_{so.custom_marketplace_order_id}",
            order_id=so.custom_marketplace_order_id,
            access_token=access_token
        )

    return f"{len(sales_orders)} Address Sync Jobs Started"
    
      

@frappe.whitelist()
def create_and_set_address(doc,method):
    
    order_id=doc.custom_marketplace_order_id

    headers = {
        "API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    request_url = f"{BASE_URL}/shipments/external_shipment_id/{order_id}"
    response = requests.get(request_url, headers=headers)

    if response.status_code != 200:
        frappe.throw(response.text)

    data = response.json()["ship_to"]
    address = data
    country_code = address.get("country_code")
    
    country_name=frappe.get_all("Country",filters={"code":country_code.lower()})
    
    line2 = address.get("address_line2")
    line3 = address.get("address_line3")

    customer_address = frappe.new_doc("Address")
    customer_address.address_title = f"{address.get('name')}"
    customer_address.address_type = "Shipping"
    customer_address.address_line1 = address.get("address_line1")

    if line2 and line3:
        customer_address.address_line2 = f"{line2} {line3}"
    elif line2:
        customer_address.address_line2 = line2
    elif line3:
        customer_address.address_line2 = line3
    else:
        customer_address.address_line2 = ""
        
    customer_address.city = address.get("city_locality")
    customer_address.country = country_name[0].name
    customer_address.state = address.get("state_province")
    customer_address.pincode = address.get("postal_code")
    customer_address.email_id = address.get("email")

    customer_address.append("links", {
        "link_doctype": "Customer",
        "link_name": address.get('name')
    })

    customer_address.insert(ignore_permissions=True)

    sales_orders = frappe.get_all(
        "Sales Order",
        filters={"custom_marketplace_order_id": order_id},
        fields=["name"]
    )

    if not sales_orders:
        frappe.throw("No Sales Order found for this Marketplace Order ID")

    sales_order_doc = frappe.get_doc("Sales Order", sales_orders[0].name)
    sales_order_doc.customer_address = customer_address.name
    sales_order_doc.shipping_address_name= customer_address.name
    sales_order_doc.save(ignore_permissions=True)

    frappe.db.commit()

    return "Address Synced Successfully"

