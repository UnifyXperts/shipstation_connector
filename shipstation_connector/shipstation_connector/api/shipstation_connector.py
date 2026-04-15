import frappe
import requests
import json
import us
from erpnext.selling.doctype.sales_order.sales_order import make_delivery_note
import requests
from frappe.utils import nowdate
from datetime import datetime
import pytz


settings = frappe.get_single("Shipstation Settings")
BASE_URL = settings.shipstation_endpoint
API_KEY = settings.get_password("v2_api_key")
NOTIFY_SELLER = settings.notify_seller
ACCOUNT_MAP=settings.account_mapper
update_tracking_info = settings.update_tracking_info_on_marketplace


shipping_account = next(
    (c for c in ACCOUNT_MAP if c.account_head=='shipment_cost'),
    None
)

carrier_row = next(
    (c for c in settings.carriers if c.is_active and c.is_default),
    None
)

def make_delivery_note_from_so(so):

    dn = make_delivery_note(so.name)

    dn.posting_date = nowdate()

    return dn

@frappe.whitelist()
def update_info_to_marketplace(marketplace,delivery_note):
    delivery_note_document=frappe.get_doc("Delivery Note",delivery_note)
    
    

def shipstation_config():
    doc = frappe.get_single("Shipstation Settings")

    if not doc.enabled:
        frappe.throw("Your ShipStation Configuration is not enabled")

    return {
        "base_url": doc.shipstation_endpoint.rstrip("/"),
        "create_sales_order": doc.create_sales_order,
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "API-Key": doc.get_password("v2_api_key")

        }
    }




@frappe.whitelist()
def update_carriers():
    config=shipstation_config()
    
    url=f"{config["base_url"]}/carriers"
    
    response=requests.get(
        url,
        headers=config["headers"]
    )

    if response.status_code not in (200, 201):
        frappe.throw(response.text)

    return response.json()

def send_so_to_shipstation():

    sales_orders = frappe.get_list(
        "Sales Order",
        filters={"docstatus": 0},
        fields=["name"]
    )

    for so in sales_orders:
        create_so({"name": so.name})
        
@frappe.whitelist(allow_guest=True)
def shipstation_label_created():
    frappe.set_user("Administrator")

    raw_data = frappe.request.get_data(as_text=True)

    if not raw_data:
        frappe.log_error("No data received", "ShipStation Webhook")
        return "No data received"

    try:
        payload = json.loads(raw_data)

        log = frappe.new_doc("Shipstation Webhook Log")
        log.raw_body = raw_data
        log.insert(ignore_permissions=True)

    except Exception:
        frappe.log_error(raw_data, "ShipStation Invalid JSON")
        return "Invalid JSON"

    resource_url = payload.get("resource_url")

    if not resource_url:
        frappe.log_error(raw_data, "Missing resource_url")
        return "No resource_url"

    config = shipstation_config()

    try:
        response = requests.get(resource_url, headers=config["headers"], timeout=15)
        label_data = response.json()
    except Exception:
        frappe.log_error(frappe.get_traceback(), "ShipStation API Failed")
        return "API Failed"

    frappe.log_error(frappe.as_json(label_data), "ShipStation Response")

    labels = label_data.get("labels") or [label_data]

    for label in labels:

        try:
            external_shipment_id = label.get("external_shipment_id")
            tracking_number = label.get("tracking_number")
            tracking_url = label.get("tracking_url")
            carrier_name = label.get("carrier_code")
            carrier_id = label.get("carrier_id")

            if not external_shipment_id:
                frappe.log_error(frappe.as_json(label), "Missing Shipment ID")
                continue

            # ✅ Get or create SO
            so_name = get_or_create_sales_order(external_shipment_id)

            if not so_name:
                frappe.log_error(external_shipment_id, "SO NOT FOUND")
                continue

            so = frappe.get_doc("Sales Order", so_name)

            if so.docstatus == 0:
                try:
                    so.save(ignore_permissions=True)
                    so.submit()
                    so.db_set("status", "Deliver And Bill", update_modified=False)

                except Exception:
                    frappe.log_error(frappe.get_traceback(), "SO Submit Failed")
                    continue

            # ✅ Check existing DN
            existing_dn = frappe.db.get_value(
                "Delivery Note Item",
                {"against_sales_order": so.name},
                "parent"
            )

            if existing_dn:
                frappe.log_error(f"DN already exists for {so.name}", "DN SKIPPED")
                continue

            # ✅ Create DN
            try:
                dn = make_delivery_note(so.name)
            except Exception:
                frappe.log_error(str(frappe.get_traceback()), "make_delivery_note FAILED")
                continue

            dn.posting_date = nowdate()
            dn.custom_tracking_number = tracking_number
            dn.custom_tracking_url = tracking_url
            dn.custom_linked_etsy_shiping_id = external_shipment_id
            dn.custom_carrier_name = carrier_name
            dn.custom_note_to_buyer = so.custom_note_from_seller
            dn.custom_carrier_id = carrier_id
            dn.custom_processed_webhook_url = resource_url

            shipment_cost = label.get("shipment_cost", {})
            shipment_amount = shipment_cost.get("amount")

            if shipment_amount:
                try:
                    shipping_account = frappe.get_value(
                        "Account",
                        {"account_name": "Shipping Charges"},
                        "name"
                    )

                    if shipping_account:
                        dn.append("taxes", {
                            "charge_type": "Actual",
                            "account_head": shipping_account,
                            "description": "ShipStation Shipping Cost",
                            "tax_amount": shipment_amount
                        })
                except Exception:
                    frappe.log_error(frappe.get_traceback(), "Shipping Tax Failed")

            # ✅ Packages
            packages = label.get("packages", [])

            for pkg in packages:
                dims = pkg.get("dimensions", {})
                weight = pkg.get("weight", {})

                dn.custom_uom_for_dimension = dims.get("unit")
                dn.custom_uom_for_weight = weight.get("unit")

                dn.append("custom_packages", {
                    "length": dims.get("length") or 0,
                    "width": dims.get("width") or 0,
                    "height": dims.get("height") or 0,
                    "weight": weight.get("value") or 0,
                    "count": 1
                })

            try:
                dn.save(ignore_permissions=True)
                dn.submit()
                dn.db_set("per_billed", 100, update_modified=False)
                dn.db_set("status", "Completed", update_modified=False)
                so.db_set("status", "Deliver And Bill", update_modified=False)
                
                frappe.db.commit()

                frappe.log_error(f"DN CREATED: {dn.name}", "SUCCESS")

            except Exception:
                frappe.log_error(frappe.get_traceback(), "DN Save/Submit Failed")
                continue

        except Exception:
            frappe.log_error(frappe.get_traceback(), "FULL LOOP FAILED")
            continue

    return "Webhook Processed"

def get_or_create_sales_order(receipt_id):

    existing_so = frappe.db.exists(
        "Sales Order",
        {"custom_marketplace_order_id": receipt_id}
    )

    if existing_so:
        return existing_so

    settings = frappe.get_single("Etsy Settings")

    response = create_single_sales_order(
        receipt_id=receipt_id
    )

    if response.get("status") != "success":

        frappe.log_error(
            json.dumps(response, indent=2),
            "Etsy SO Creation Failed"
        )

        return None

    return response.get("sales_order")


def build_shipment_items(so):
    items = []

    for item in so.items:
        items.append({
            "quantity": int(item.qty),
            "name": item.item_name,
            "sku": item.item_code or "",
            "unit_price": float(item.rate),
            "weight": {
                "value": float(item.weight_per_unit or 0),
                "unit": "ounce"
            },
            "options": []
        })

    return items


def build_order_payload(so):
    return {
        "order_number": so.name,
        "order_date": so.transaction_date.strftime("%Y-%m-%dT00:00:00Z"),
        "order_status": "awaiting_shipment",
        "amount_paid": float(so.grand_total),
        "items": build_shipment_items(so)
    }


def build_packages(so):
    total_weight = 0

    for item in so.items:
        total_weight += (item.weight_per_unit or 0) * item.qty

    return [
        {
            "weight": {
                "value": float(total_weight or 1),
                "unit": "ounce"
            }
        }
    ]

@frappe.whitelist()
def create_single_sales_order(receipt_id):

    BASE_URL = "https://openapi.etsy.com"

    try:
        settings = frappe.get_single("Etsy Settings")

        shop_id = settings.shop_id
        access_token = settings.get_password("access_token")
        api_key = settings.client_id
        user_id = settings.user_id

        response = requests.get(
            f"{BASE_URL}/v3/application/shops/{shop_id}/receipts/{receipt_id}",
            headers={
                "Authorization": f"Bearer {access_token}",
                "x-api-key": api_key
            },
            timeout=20
        )

        if response.status_code != 200:
            return {
                "status": "error",
                "message": f"Etsy API Error: {response.text}"
            }

        receipt_data = response.json()

        if not isinstance(receipt_data, dict):
            return {
                "status": "error",
                "message": "Invalid receipt response from Etsy"
            }

        # -----------------------------
        # CHECK EXISTING SALES ORDER
        # -----------------------------
        existing_so = frappe.db.get_value(
            "Sales Order",
            {"custom_marketplace_order_id": receipt_id},
            "name"
        )

        # -----------------------------
        # CUSTOMER
        # -----------------------------
        customer_name = (
            receipt_data.get("name")
            or receipt_data.get("buyer_name")
            or "Etsy Customer"
        )

        customer = frappe.db.get_value(
            "Customer",
            {"customer_name": customer_name}
        )

        if not customer:
            customer_doc = frappe.get_doc({
                "doctype": "Customer",
                "customer_name": customer_name,
                "customer_type": "Individual"
            })
            customer_doc.insert(ignore_permissions=True)
            customer = customer_doc.name

        # -----------------------------
        # ADDRESS CHECK
        # -----------------------------
        is_id_present = check_address_from_shipstation(order_id=receipt_id)

        if not is_id_present:
            return {
                "status": "fail",
                "message": "Address Not Found in Shipstation, Skipping..."
            }

        create_contact_if_not_exists(customer, receipt_data)
        create_address_if_not_exists(customer, receipt_data)

        # -----------------------------
        # DATES
        # -----------------------------
        created_ts = receipt_data.get("created_timestamp")

        if created_ts:
            transaction_date = datetime.fromtimestamp(
                created_ts, pytz.UTC
            ).date()
        else:
            transaction_date = frappe.utils.today()

        delivery_date = transaction_date

        # -----------------------------
        # ITEMS
        # -----------------------------
        items = []
        transactions = receipt_data.get("transactions", [])

        discount = 0
        discount_data = receipt_data.get("discount_amt")

        if discount_data:
            discount = (
                discount_data.get("amount", 0)
                / discount_data.get("divisor", 1)
            )

        for tx in transactions:

            item_code = tx.get("sku") or str(tx.get("transaction_id"))

            if not frappe.db.exists("Item", item_code):

                item_name = tx.get("title") or item_code

                item_doc = frappe.get_doc({
                    "doctype": "Item",
                    "item_code": item_code,
                    "item_name": item_name,
                    "item_group": "Products",
                    "stock_uom": "Nos",
                    "is_stock_item": 0,
                    "is_sales_item": 1
                })

                item_doc.insert(ignore_permissions=True)

            rate = 0
            price_data = tx.get("price")

            if isinstance(price_data, dict):
                amount = price_data.get("amount", 0)
                divisor = price_data.get("divisor", 1)

                if divisor:
                    rate = amount / divisor

            items.append({
                "item_code": item_code,
                "qty": tx.get("quantity", 1),
                "rate": rate
            })

            expected_ts = tx.get("expected_ship_date")

            if expected_ts:
                delivery_date = datetime.fromtimestamp(
                    expected_ts, pytz.UTC
                ).date()

        if not items:
            return {
                "status": "error",
                "message": "No valid items found in receipt"
            }

        if delivery_date <= transaction_date:
            delivery_date = frappe.utils.add_days(transaction_date, 1)

        # -----------------------------
        # TAX
        # -----------------------------
        tax_amount = 0
        tax_data = receipt_data.get("total_tax_cost")

        frappe.log_error(frappe.as_json(tax_data), "Etsy Tax Data")

        if tax_data:
            tax_amount = (
                tax_data.get("amount", 0)
                / tax_data.get("divisor", 1)
            )

        note_from_seller = receipt_data.get("message_from_seller") or ""

        # -----------------------------
        # UPDATE EXISTING SO
        # -----------------------------
        if existing_so:

            so = frappe.get_doc("Sales Order", existing_so)

            so.custom_note_from_seller = note_from_seller
            so.discount_amount = discount

            so.set("taxes", [])

            if tax_amount:
                so.append("taxes", {
                    "charge_type": "Actual",
                    "account_head": "Sales Tax - RD",
                    "tax_amount": tax_amount,
                    "description": "Etsy Sales Tax"
                })

            so.save(ignore_permissions=True)

            return {
                "status": "success",
                "message": "Sales Order Updated",
                "sales_order": so.name
            }

        # -----------------------------
        # CREATE NEW SALES ORDER
        # -----------------------------
        sales_order = frappe.get_doc({
            "doctype": "Sales Order",
            "customer": customer,
            "po_no": receipt_id,
            "transaction_date": transaction_date,
            "delivery_date": delivery_date,
            "custom_marketplace_order_id": receipt_id,
            "custom_marketplace": "Etsy",
            "custom_shop_id": shop_id,
            "custom_seller_id": user_id,
            "custom_note_from_seller": note_from_seller,
            "discount_amount": discount,
            "items": items,
            
        })

        if tax_amount:
            sales_order.append("taxes", {
                "charge_type": "Actual",
                "account_head": "Sales Tax - RD",
                "tax_amount": tax_amount,
                "description": "Etsy Sales Tax"
            })

        sales_order.insert(ignore_permissions=True)
        sales_order.submit()

        return {
            "status": "success",
            "message": "Sales Order Created Successfully",
            "sales_order": sales_order.name
        }

    except Exception:
        frappe.log_error(
            frappe.get_traceback(),
            "Etsy Create Single Sales Order Error"
        )

        return {
            "status": "error",
            "message": str(frappe.get_traceback())
        }

def check_address_from_shipstation(order_id):
    
    config=shipstation_config()
    headers=config.get("headers")
    BASE_URL=config.get("base_url")
    request_url = f"{BASE_URL}/shipments/external_shipment_id/{order_id}"
    response = requests.get(request_url, headers=headers)

    if response.status_code != 200:
        frappe.throw(response.text)

    data = response.json()["ship_to"]
    
    if data:
        return True;
    
    else:
        return False
    
def create_contact_if_not_exists(customer, receipt):

    email = receipt.get("buyer_email")
    if not email:
        return

    if frappe.db.exists("Contact Email", {"email_id": email}):
        return

    contact = frappe.get_doc({
        "doctype": "Contact",
        "first_name": receipt.get("name") or "Etsy Customer",
        "email_ids": [{
            "email_id": email,
            "is_primary": 1
        }],
        "links": [{
            "link_doctype": "Customer",
            "link_name": customer
        }]
    })

    contact.insert(ignore_permissions=True)


def create_address_if_not_exists(customer, receipt):

    if not receipt.get("first_line"):
        return

    address_title = f"{customer}-Etsy"

    if frappe.db.exists("Address", address_title):
        return

    address = frappe.get_doc({
        "doctype": "Address",
        "address_title": address_title,
        "address_type": "Shipping",
        "address_line1": receipt.get("first_line"),
        "address_line2": receipt.get("second_line"),
        "city": receipt.get("city"),
        "state": receipt.get("state"),
        "pincode": receipt.get("zip"),
        "country": receipt.get("country_iso"),
        "links": [{
            "link_doctype": "Customer",
            "link_name": customer
        }]
    })

    address.insert(ignore_permissions=True)
      
@frappe.whitelist()
def create_so(doc=None,method=None,payload=None):

    import frappe
    import requests
    from datetime import datetime
    
    config = shipstation_config()
    
    if isinstance(payload, dict):
        so = frappe.get_doc("Sales Order", payload.get("name"))
    else:
        so = payload

        # so = frappe.get_doc("Sales Order", payload.get("name"))
    
    if so.custom_synced_to_shipstation:
        frappe.log_error("Sync Error","This sales order is already Synced to shipstation ,skipping..........")
        return

    ship_to = get_address_dict(so.customer_address)
    ship_from = get_company_address_dict(so.company)

    settings = frappe.get_single("Shipstation Settings")

    carrier_row = next(
        (c for c in settings.carriers if c.is_active and c.is_default),
        None
    )

    if not carrier_row:
        frappe.log_error("No active default carrier found in Shipstation Settings","Carrier Error")

    # -----------------------------
    # Helper: Convert to datetime
    # -----------------------------
    def to_datetime(val):
        if not val:
            return None
        if isinstance(val, datetime):
            return val
        return datetime.combine(val, datetime.min.time())

    # -----------------------------
    # Date Handling (FIXED)
    # -----------------------------
    utc_now = datetime.utcnow()

    delivery_date_iso = None
    shipment_date = None
    order_date = None
    hold_until_date = None

    # Ship By (Deliver By)
    delivery_dt = to_datetime(so.delivery_date)
    if delivery_dt:
        delivery_date_iso = delivery_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Ship Date (IMPORTANT FIX)
    ship_dt = to_datetime(so.custom_shipment_date)

    if ship_dt:
        if ship_dt < utc_now:
            ship_dt = utc_now  # prevent past date error
    else:
        ship_dt = utc_now  # fallback

    shipment_date = ship_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    payment_dt = to_datetime(so.custom_payment_date)
    if payment_dt:
        order_date = payment_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    hold_dt = to_datetime(getattr(so, "custom_hold_until", None))
    if hold_dt:
        hold_until_date = hold_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # -----------------------------
    # Payload
    # -----------------------------
    shipstation_payload = {
        "shipments": [
            {
                "validate_address": "no_validation",
                "external_shipment_id": so.custom_marketplace_order_id,
                "carrier_id": carrier_row.carrier_id,
                "create_sales_order": bool(config["create_sales_order"]),
                "store_id": None,
                "notes_from_buyer": so.po_no or "",
                "notes_for_gift": "",
                "is_gift": False,
                "zone": 0,
                "display_scheme": "paperless",
                "assigned_user": None,
                "shipment_status": "pending",

                "amount_paid": {
                    "currency": so.currency,
                    "amount": float(so.grand_total)
                },
                "shipping_paid": {
                    "currency": so.currency,
                    "amount": float(get_shipping_amount(so))
                },
                "tax_paid": {
                    "currency": so.currency,
                    "amount": float(so.total_taxes_and_charges or 0)
                },

                "ship_by_date": delivery_date_iso,
                "ship_date": shipment_date,
                "order_date": order_date,
                "hold_until_date": hold_until_date,

                "ship_to": ship_to,
                "ship_from": ship_from,
                "items": build_shipment_items(so),
                "packages": build_packages(so)
            }
        ]
    }

    # -----------------------------
    # API Call
    # -----------------------------
    # so.save(ignore_permissions=True)

    url = f"{config['base_url']}/shipments"
    response = requests.post(
        url,
        headers=config["headers"],
        json=shipstation_payload,
        timeout=30
    )

    if response.status_code not in (200, 201):
        # so.save(ignore_permissions=True)
        frappe.throw(f"ShipStation API Error: {response.text}")

    if hasattr(so, 'custom_shipstation_response'):
        so.custom_shipstation_response = frappe.as_json(response.json())
    
    so.custom_synced_to_shipstation = 1
    so.db_update()
    # frappe.db.commit()

    return response.status_code,response.json()


def build_shipment_items(so):
    items = []

    for item in so.items:
        # Calculate tax per unit if needed
        unit_tax = 0
        if hasattr(so, 'taxes') and so.taxes and item.qty > 0:
            # Simplified - you may need to calculate actual item tax
            unit_tax = float(so.total_taxes_and_charges or 0) / item.qty if item.qty > 0 else 0

        items.append({
            "sku": item.item_code or "",
            "name": item.item_name,
            "quantity": int(item.qty),
            "unit_price": float(item.rate),  # This remains a simple number
            "unit_tax": unit_tax,  # This remains a simple number
            "weight": {
                "value": float(item.weight_per_unit or 0),
                "unit": "ounce"
            },
            "product_id": item.item_code,
            "fulfillment_sku": item.item_code,
            "adjustment": False,
            "upc": "",
            "create_product": True,
            "options": []
        })

    return items


def build_packages(so):
    packages = []
    
    total_weight = 0
    for item in so.items:
        total_weight += (item.weight_per_unit or 0) * item.qty

    package = {
        "package_id": None,
        "package_code": "package",
        "package_name": "Default Package",
        "weight": {
            "value": float(total_weight or 1),
            "unit": "pound"
        },
        "dimensions": {
            "unit": "inch",
            "length": 12,
            "width": 12,
            "height": 12
        },
        "insured_value": {
            "currency": so.currency,
            "amount": float(so.grand_total)
        },
        "label_messages": {
            "reference1": so.name,
            "reference2": so.customer_name,
            "reference3": ""
        },
        "external_package_id": f"PKG-{so.name}",
        "content_description": "Goods",
        "products": []
    }

    packages.append(package)
    return packages
def get_shipping_amount(so):
    """Extract shipping amount from taxes"""
    shipping_amount = 0
    
    if hasattr(so, 'taxes') and so.taxes:
        for tax in so.taxes:
            # Check if this is a shipping charge
            if tax.charge_type == "Actual" and tax.description and "shipping" in tax.description.lower():
                shipping_amount = float(tax.tax_amount or 0)
                break
    
    return shipping_amount
def get_address_dict(address_name):
    if not address_name:
        frappe.throw("Customer Address is required")

    address = frappe.get_doc("Address", address_name)
    country_code = get_country_code(address.country)

    return {
        "name": address.address_title,
        "phone": safe_phone(address.phone),
        "email":address.email_id,
        "address_line1": address.address_line1,
        "address_line2": address.address_line2 or "",
        "city_locality": address.city,
        "state_province": get_state_code(address.state, country_code),
        "postal_code": address.pincode,
        "country_code": country_code
    }


def get_company_address_dict(company_name):
    address_name = frappe.db.get_value(
        "Dynamic Link",
        {
            "link_doctype": "Company",
            "link_name": company_name,
            "parenttype": "Address"
        },
        "parent"
    )

    if not address_name:
        frappe.throw(f"No address linked to Company {company_name}")

    address = frappe.get_doc("Address", address_name)
    country_code = get_country_code(address.country)

    return {
        "name": company_name,
        "phone": safe_phone(address.phone),
        "email": address.email_id,
        "company_name": company_name,
        "address_line1": address.address_line1,
        "address_line2": address.address_line2 or "",
        "address_line3": "",
        "city_locality": address.city,
        "state_province": get_state_code(address.state, country_code),
        "postal_code": address.pincode,
        "country_code": country_code,
        "address_residential_indicator": "yes" if address.address_type == "Residential" else "no",
        "instructions": "",
        "geolocation": []
    }


def get_country_code(country_name):
    code = frappe.db.get_value("Country", country_name, "code")

    if not code:
        frappe.throw(f"ISO Country Code not set for {country_name}")

    return code


def get_state_code(state_name, country_code):
    if not state_name:
        frappe.throw("State is required")

    if country_code == "US":
        state_name = state_name.strip().title()
        state = us.states.lookup(state_name)

        if state:
            return state.abbr

        if len(state_name) == 2:
            return state_name.upper()

        frappe.throw(f"Invalid US state: {state_name}")

    return state_name


def safe_phone(phone):
    return phone if phone else "NA"

import frappe
import traceback

@frappe.whitelist(allow_guest=True)
def sync_sales_order_to_shipstation():
    
    try:
        settings = frappe.get_single("Shipstation Settings")
        sync_so_to_shipstation = settings.sync_so_to_shipstation

        if not sync_so_to_shipstation:
            frappe.log_error(
                title="ShipStation Sync Disabled",
                message="Auto syncing of Sales Orders to ShipStation is disabled in settings."
            )
            return

        sales_orders = frappe.get_all(
            "Sales Order",
            filters={"custom_synced_to_shipstation": 0},
            fields=["name"]
        )

        if not sales_orders:
            frappe.log_error(
                title="No Sales Orders Found",
                message="No Sales Orders found that need syncing to ShipStation."
            )
            return

        for so in sales_orders:
            try:
                status_code, response = create_so(doc=None,method=None,payload={"name": so.get("name")})
                frappe.log_error("processed", f"{response} {status_code}")

                if status_code in (200, 201):
                    frappe.db.set_value(
                        "Sales Order",
                        so.get("name"),
                        "custom_synced_to_shipstation",
                        1
                    )
                    frappe.db.commit()

            except Exception as e:
                frappe.log_error(
                    title=f"ShipStation Sync Failed for SO: {so.get('name')}",
                    message=f"""
            Error while syncing Sales Order: {so.get('name')}

            Error: {str(e)}

            Traceback:
            {traceback.format_exc()}
            """
                )
    except Exception as e:
        frappe.log_error("Error", str(e))


        
def process_shipstation_logs_bg():

    logs = frappe.get_all(
        "Shipstation Webhook Log",
        filters={"processed": 0},
        fields=["name", "raw_body"],
        order_by="creation asc"
    )

    if not logs:
        frappe.log_error("No pending logs", "Shipstation BG Job")
        return

    for log in logs:
        try:
            raw = log.raw_body

            payload = json.loads(raw) if isinstance(raw, str) else raw

            process_shipstation_payload(payload)

            frappe.db.set_value(
                "Shipstation Webhook Log",
                log.name,
                "processed",
                1
            )

            frappe.db.commit()

        except Exception:
            frappe.log_error(
                frappe.get_traceback(),
                f"BG Sync Failed: {log.name}"
            )

            frappe.db.set_value(
                "Shipstation Webhook Log",
                log.name,
                "processed",
                0
            )

            frappe.db.commit()

@frappe.whitelist()
def trigger_shipstation_sync():
    
    frappe.enqueue(
        "shipstation_connector.shipstation_connector.api.shipstation_connector.process_shipstation_logs_bg",
        queue="long",
        timeout=600
    )

    return "Shipstation sync started in background"

def process_shipstation_payload(payload):

    resource_url = payload.get("resource_url")

    if not resource_url:
        frappe.log_error(payload, "Missing resource_url")
        return

    config = shipstation_config()

    try:
        response = requests.get(resource_url, headers=config["headers"], timeout=15)
        label_data = response.json()
    except Exception:
        frappe.log_error(frappe.get_traceback(), "ShipStation API Failed")
        return

    labels = label_data.get("labels") or [label_data]

    for label in labels:
        try:
            external_shipment_id = label.get("external_shipment_id")

            if not external_shipment_id:
                continue

            so_name = get_or_create_sales_order(external_shipment_id)

            if not so_name:
                continue

            so = frappe.get_doc("Sales Order", so_name)

            if so.docstatus == 0:
                try:
                    so.submit()
                except:
                    continue

            existing_dn = frappe.db.get_value(
                "Delivery Note Item",
                {"against_sales_order": so.name},
                "parent"
            )

            if existing_dn:
                continue

            dn = make_delivery_note(so.name)

            dn.posting_date = nowdate()
            dn.custom_tracking_number = label.get("tracking_number")
            dn.custom_tracking_url = label.get("tracking_url")
            dn.custom_linked_etsy_shiping_id = external_shipment_id
            dn.custom_processed_webhook_url = resource_url

            dn.save(ignore_permissions=True)
            dn.submit()

            frappe.db.commit()

        except Exception:
            frappe.log_error(frappe.get_traceback(), "Payload Processing Failed")
            continue