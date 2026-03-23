import frappe
from xml.etree.ElementTree import Element, tostring

def get_context(context):
    frappe.local.no_cache = 1

    root = Element("Orders")
    xml_bytes = tostring(root, encoding="utf-8")
    xml_string = '<?xml version="1.0" encoding="UTF-8"?>' + xml_bytes.decode("utf-8")

    frappe.local.response.headers["Content-Type"] = "application/xml"
    frappe.local.response.response = xml_string

    return ""