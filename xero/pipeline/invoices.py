from xero.pipeline.interface import Pipeline
from xero.pipeline.utils import parse_timestamp

pipeline = Pipeline(
    "Invoices",
    "Invoices",
    lambda x: x["Invoices"],
    lambda rows: [
        {
            "Type": row.get("Type"),
            "InvoiceID": row.get("InvoiceID"),
            "InvoiceNumber": row.get("InvoiceNumber"),
            "Payments": [
                {
                    "PaymentID": payment.get("PaymentID"),
                    "Date": payment.get("Date"),
                    "Amount": payment.get("Amount"),
                }
                for payment in row["Payments"]
            ]
            if row.get("Payments")
            else [],
            "AmountDue": row.get("AmountDue"),
            "AmountPaid": row.get("AmountPaid"),
            "AmountCredited": row.get("AmountCredited"),
            "HasAttachments": row.get("HasAttachments"),
            "RepeatingInvoiceID": row.get("RepeatingInvoiceID"),
            "Contact": {
                "ContactID": row["Contact"].get("ContactID"),
            }
            if row.get("Contact")
            else {},
            "DateString": row.get("DateString"),
            "DueDateString": row.get("DueDateString"),
            "BrandingThemeID": row.get("BrandingThemeID"),
            "Status": row.get("Status"),
            "LineAmountTypes": row.get("LineAmountTypes"),
            "LineItems": [
                {
                    "ItemCode": line_item.get("ItemCode"),
                    "Description": line_item.get("Description"),
                    "Quantity": line_item.get("Quantity"),
                    "UnitAmount": line_item.get("UnitAmount"),
                    "TaxType": line_item.get("TaxType"),
                    "TaxAmount": line_item.get("TaxAmount"),
                    "LineAmount": line_item.get("LineAmount"),
                    "AccountCode": line_item.get("AccountCode"),
                    "Item": {
                        "ItemID": line_item["Item"].get("ItemID"),
                        "Name": line_item["Item"].get("Name"),
                        "Code": line_item["Item"].get("Code"),
                    },
                    "Tracking": [
                        {
                            "TrackingCategoryID": tracking.get("TrackingCategoryID"),
                            "Name": tracking.get("Name"),
                            "Option": tracking.get("Option"),
                        }
                        for tracking in line_item["Tracking"]
                    ]
                    if line_item.get("Tracking")
                    else [],
                    "LineItemID": line_item.get("LineItemID"),
                }
                for line_item in row["LineItems"]
            ]
            if row.get("LineItems")
            else [],
            "SubTotal": row.get("SubTotal"),
            "TotalTax": row.get("TotalTax"),
            "Total": row.get("Total"),
            "UpdatedDateUTC": parse_timestamp(row.get("UpdatedDateUTC")),
            "CurrencyCode": row.get("CurrencyCode"),
            "FullyPaidOnDate": row.get("FullyPaidOnDate"),
        }
        for row in rows
    ],
    [
        {"name": "Type", "type": "STRING"},
        {"name": "InvoiceID", "type": "STRING"},
        {"name": "InvoiceNumber", "type": "STRING"},
        {
            "name": "Payments",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "PaymentID", "type": "STRING"},
                {"name": "Date", "type": "STRING"},
                {"name": "Amount", "type": "NUMERIC"},
            ],
        },
        {"name": "AmountDue", "type": "NUMERIC"},
        {"name": "AmountPaid", "type": "NUMERIC"},
        {"name": "AmountCredited", "type": "NUMERIC"},
        {"name": "HasAttachments", "type": "BOOLEAN"},
        {"name": "RepeatingInvoiceID", "type": "STRING"},
        {
            "name": "Contact",
            "type": "RECORD",
            "fields": [
                {"name": "ContactID", "type": "STRING"},
            ],
        },
        {"name": "DateString", "type": "TIMESTAMP"},
        {"name": "DueDateString", "type": "TIMESTAMP"},
        {"name": "Status", "type": "STRING"},
        {"name": "LineAmountTypes", "type": "STRING"},
        {
            "name": "LineItems",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "Description", "type": "STRING"},
                {"name": "UnitAmount", "type": "NUMERIC"},
                {"name": "TaxType", "type": "STRING"},
                {"name": "TaxAmount", "type": "NUMERIC"},
                {"name": "LineAmount", "type": "NUMERIC"},
                {"name": "AccountCode", "type": "STRING"},
                {"name": "Quantity", "type": "NUMERIC"},
                {
                    "name": "Item",
                    "type": "RECORD",
                    "fields": [
                        {"name": "ItemID", "type": "STRING"},
                        {"name": "Name", "type": "STRING"},
                        {"name": "Code", "type": "STRING"},
                    ],
                },
                {
                    "name": "Tracking",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {"name": "TrackingCategoryID", "type": "STRING"},
                        {"name": "Name", "type": "STRING"},
                        {"name": "Option", "type": "STRING"},
                    ],
                },
                {"name": "LineItemID", "type": "STRING"},
            ],
        },
        {"name": "SubTotal", "type": "NUMERIC"},
        {"name": "TotalTax", "type": "NUMERIC"},
        {"name": "Total", "type": "NUMERIC"},
        {"name": "UpdatedDateUTC", "type": "TIMESTAMP"},
        {"name": "CurrencyCode", "type": "STRING"},
    ],
    "InvoiceID",
)
