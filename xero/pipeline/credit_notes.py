from xero.pipeline.interface import Pipeline
from xero.pipeline.utils import parse_timestamp
from xero.pipeline.headers import timeframe

pipeline = Pipeline(
    name="CreditNotes",
    headers_fn=timeframe,
    uri="api.xro/2.0/CreditNotes",
    res_fn=lambda x: x["CreditNotes"],
    transform=lambda rows: [
        {
            "CreditNoteID": row.get("CreditNoteID"),
            "CreditNoteNumber": row.get("CreditNoteNumber"),
            "Type": row.get("Type"),
            "RemainingCredit": row.get("RemainingCredit"),
            "Contact": {
                "ContactID": row["Contact"].get("ContactID"),
            }
            if row.get("Contact")
            else {},
            "DateString": row.get("DateString"),
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
        }
        for row in rows
    ],
    schema=[
        {"name": "CreditNoteID", "type": "STRING"},
        {"name": "CreditNoteNumber", "type": "STRING"},
        {"name": "Type", "type": "STRING"},
        {"name": "RemainingCredit", "type": "NUMERIC"},
        {
            "name": "Contact",
            "type": "RECORD",
            "fields": [
                {"name": "ContactID", "type": "STRING"},
            ],
        },
        {"name": "DateString", "type": "TIMESTAMP"},
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
    ],
    id_key="CreditNoteID",
)
