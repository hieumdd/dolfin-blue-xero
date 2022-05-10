from xero.pipeline.interface import Pipeline
from xero.pipeline.utils import parse_timestamp
from xero.pipeline.headers import timeframe

pipeline = Pipeline(
    "Invoices",
    headers_fn=timeframe,
    uri="api.xro/2.0/Invoices",
    res_fn=lambda x: x["Invoices"],
    transform=lambda rows: [
        {
            "Type": row.get("Type"),
            "InvoiceID": row.get("InvoiceID"),
            "InvoiceNumber": row.get("InvoiceNumber"),
            "CreditNotes": [
                {
                    "CreditNoteID": credit_note.get("CreditNoteID"),
                }
                for credit_note in row["CreditNotes"]
            ]
            if row.get("CreditNotes")
            else {},
            "AmountDue": row.get("AmountDue"),
            "AmountPaid": row.get("AmountPaid"),
            "AmountCredited": row.get("AmountCredited"),
            "Contact": {
                "ContactID": row["Contact"].get("ContactID"),
            }
            if row.get("Contact")
            else {},
            "DateString": row.get("DateString"),
            "DueDateString": row.get("DueDateString"),
            "Status": row.get("Status"),
            "LineAmountTypes": row.get("LineAmountTypes"),
            "LineItems": [
                {
                    "ItemCode": line_item.get("ItemCode"),
                    "Description": line_item.get("Description"),
                    "UnitAmount": line_item.get("UnitAmount"),
                    "TaxType": line_item.get("TaxType"),
                    "TaxAmount": line_item.get("TaxAmount"),
                    "LineAmount": line_item.get("LineAmount"),
                    "AccountCode": line_item.get("AccountCode"),
                    "Item": {
                        "ItemID": line_item["Item"].get("ItemID"),
                        "Name": line_item["Item"].get("Name"),
                        "Code": line_item["Item"].get("Code"),
                    }
                    if line_item.get("Item")
                    else {},
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
                    "Quantity": line_item.get("Quantity"),
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
        }
        for row in rows
    ],
    schema=[
        {"name": "Type", "type": "STRING"},
        {"name": "InvoiceID", "type": "STRING"},
        {"name": "InvoiceNumber", "type": "STRING"},
        {
            "name": "CreditNotes",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "CreditNoteID", "type": "STRING"},
            ],
        },
        {"name": "AmountDue", "type": "NUMERIC"},
        {"name": "AmountPaid", "type": "NUMERIC"},
        {"name": "AmountCredited", "type": "NUMERIC"},
        {
            "name": "Contact",
            "type": "RECORD",
            "fields": [
                {"name": "ContactID", "type": "STRING"},
            ],
        },
        {"name": "Date", "type": "STRING"},
        {"name": "DueDate", "type": "STRING"},
        {"name": "DateString", "type": "TIMESTAMP"},
        {"name": "DueDateString", "type": "TIMESTAMP"},
        {"name": "Status", "type": "STRING"},
        {"name": "LineAmountTypes", "type": "STRING"},
        {
            "name": "LineItems",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "ItemCode", "type": "STRING"},
                {"name": "Description", "type": "STRING"},
                {"name": "UnitAmount", "type": "NUMERIC"},
                {"name": "TaxType", "type": "STRING"},
                {"name": "TaxAmount", "type": "NUMERIC"},
                {"name": "LineAmount", "type": "NUMERIC"},
                {"name": "AccountCode", "type": "STRING"},
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
                {"name": "Quantity", "type": "NUMERIC"},
                {"name": "LineItemID", "type": "STRING"},
            ],
        },
        {"name": "SubTotal", "type": "NUMERIC"},
        {"name": "TotalTax", "type": "NUMERIC"},
        {"name": "Total", "type": "NUMERIC"},
        {"name": "UpdatedDateUTC", "type": "STRING"},
        {"name": "CurrencyCode", "type": "STRING"},
    ],
    id_key="InvoiceID",
)
