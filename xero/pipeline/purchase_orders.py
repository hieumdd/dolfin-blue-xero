from xero.pipeline.interface import Pipeline
from xero.repo import get_listing
from xero.pipeline.utils import parse_timestamp
from xero.pipeline.headers import timeframe

pipeline = Pipeline(
    name="PurchaseOrders",
    headers_fn=timeframe,
    uri="api.xro/2.0/PurchaseOrders",
    res_fn=lambda x: x["PurchaseOrders"],
    transform=lambda rows: [
        {
            "PurchaseOrderID": row.get("PurchaseOrderID"),
            "PurchaseOrderNumber": row.get("PurchaseOrderNumber"),
            "DateString": row.get("DateString"),
            "DeliveryDateString": row.get("DeliveryDateString"),
            "DeliveryAddress": row.get("DeliveryAddress"),
            "AttentionTo": row.get("AttentionTo"),
            "Telephone": row.get("Telephone"),
            "DeliveryInstructions": row.get("DeliveryInstructions"),
            "IsDiscounted": row.get("IsDiscounted"),
            "Type": row.get("Type"),
            "CurrencyCode": row.get("CurrencyCode"),
            "Contact": {
                "ContactID": row["Contact"].get("ContactID"),
            },
            "BrandingThemeID": row.get("BrandingThemeID"),
            "Status": row.get("Status"),
            "LineAmountTypes": row.get("LineAmountTypes"),
            "LineItems": [
                {
                    "Description": line_item.get("Description"),
                    "UnitAmount": line_item.get("UnitAmount"),
                    "TaxType": line_item.get("TaxType"),
                    "TaxAmount": line_item.get("TaxAmount"),
                    "LineAmount": line_item.get("LineAmount"),
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
        {"name": "PurchaseOrderID", "type": "STRING"},
        {"name": "PurchaseOrderNumber", "type": "STRING"},
        {"name": "DateString", "type": "TIMESTAMP"},
        {"name": "DeliveryDateString", "type": "TIMESTAMP"},
        {"name": "DeliveryAddress", "type": "STRING"},
        {"name": "AttentionTo", "type": "STRING"},
        {"name": "Telephone", "type": "STRING"},
        {"name": "DeliveryInstructions", "type": "STRING"},
        {"name": "IsDiscounted", "type": "BOOLEAN"},
        {"name": "Type", "type": "STRING"},
        {"name": "CurrencyCode", "type": "STRING"},
        {
            "name": "Contact",
            "type": "RECORD",
            "fields": [
                {"name": "ContactID", "type": "STRING"},
            ],
        },
        {"name": "BrandingThemeID", "type": "STRING"},
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
                {"name": "Quantity", "type": "NUMERIC"},
                {"name": "LineItemID", "type": "STRING"},
            ],
        },
        {"name": "SubTotal", "type": "NUMERIC"},
        {"name": "TotalTax", "type": "NUMERIC"},
        {"name": "Total", "type": "NUMERIC"},
        {"name": "UpdatedDateUTC", "type": "TIMESTAMP"},
    ],
    id_key="PurchaseOrderID",
)
