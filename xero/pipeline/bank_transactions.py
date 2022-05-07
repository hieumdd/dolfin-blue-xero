from xero.pipeline.interface import Pipeline
from xero.pipeline.utils import parse_timestamp

pipeline = Pipeline(
    "BankTransactions",
    "BankTransactions",
    lambda x: x["BankTransactions"],
    lambda rows: [
        {
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
                    "Description": line_item.get("Description"),
                    "UnitAmount": line_item.get("UnitAmount"),
                    "TaxType": line_item.get("TaxType"),
                    "TaxAmount": line_item.get("TaxAmount"),
                    "LineAmount": line_item.get("LineAmount"),
                    "AccountCode": line_item.get("AccountCode"),
                    "Quantity": line_item.get("Quantity"),
                    "LineItemID": line_item.get("LineItemID"),
                }
                for line_item in row["LineItems"]
            ]
            if row.get("LineItems")
            else [],
            "SubTotal": row.get('"SubTotal"'),
            "TotalTax": row.get('"TotalTax"'),
            "Total": row.get('"Total"'),
            "UpdatedDateUTC": parse_timestamp(row.get('"UpdatedDateUTC"')),
            "CurrencyCode": row.get('"CurrencyCode"'),
            "BankTransactionID": row.get('"BankTransactionID"'),
            "BankAccount": {
                "AccountID": row["BankAccount"].get("AccountID"),
                "Code": row["BankAccount"].get("Code"),
                "Name": row["BankAccount"].get("Name"),
            }
            if row.get("BankAccount")
            else {},
            "BatchPayment": {
                "Account": {
                    "AccountID": row["BatchPayment"]["Account"].get("AccountID"),
                }
                if row["BatchPayment"].get("Account")
                else {},
                "BatchPaymentID": row["BatchPayment"].get("BatchPaymentID"),
                "Date": row["BatchPayment"].get("Date"),
                "Type": row["BatchPayment"].get("Type"),
                "Status": row["BatchPayment"].get("Status"),
                "TotalAmount": row["BatchPayment"].get("TotalAmount"),
                "UpdatedDateUTC": row["BatchPayment"].get("UpdatedDateUTC"),
                "IsReconciled": row["BatchPayment"].get("IsReconciled"),
            }
            if row.get("BatchPayment")
            else {},
            "Type": row.get("Type"),
            "Reference": row.get("Reference"),
            "IsReconciled": row.get("IsReconciled"),
        }
        for row in rows
    ],
    [
        {
            "name": "Contact",
            "type": "record",
            "fields": [
                {"name": "ContactID", "type": "STRING"},
            ],
        },
        {"name": "DateString", "type": "TIMESTAMP"},
        {"name": "Status", "type": "STRING"},
        {"name": "LineAmountTypes", "type": "STRING"},
        {
            "name": "LineItems",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "Description", "type": "STRING"},
                {"name": "UnitAmount", "type": "STRING"},
                {"name": "TaxType", "type": "STRING"},
                {"name": "TaxAmount", "type": "STRING"},
                {"name": "LineAmount", "type": "STRING"},
                {"name": "AccountCode", "type": "STRING"},
                {"name": "Quantity", "type": "STRING"},
                {"name": "LineItemID", "type": "STRING"},
            ],
        },
        {"name": "SubTotal", "type": "STRING"},
        {"name": "TotalTax", "type": "STRING"},
        {"name": "Total", "type": "STRING"},
        {"name": "UpdatedDateUTC", "type": "TIMESTAMP"},
        {"name": "CurrencyCode", "type": "STRING"},
        {"name": "BankTransactionID", "type": "STRING"},
        {
            "name": "BankAccount",
            "type": "record",
            "fields": [
                {"name": "AccountID", "type": "STRING"},
                {"name": "Code", "type": "STRING"},
                {"name": "Name", "type": "STRING"},
            ],
        },
        {
            "name": "BatchPayment",
            "type": "record",
            "fields": [
                {
                    "name": "Account",
                    "type": "record",
                    "fields": [
                        {"name": "AccountID", "type": "STRING"},
                    ],
                },
                {"name": "BatchPaymentID", "type": "STRING"},
                {"name": "Date", "type": "STRING"},
                {"name": "Type", "type": "STRING"},
                {"name": "Status", "type": "STRING"},
                {"name": "TotalAmount", "type": "STRING"},
                {"name": "UpdatedDateUTC", "type": "STRING"},
                {"name": "IsReconciled", "type": "STRING"},
            ],
        },
        {"name": "Type", "type": "STRING"},
        {"name": "Reference", "type": "STRING"},
        {"name": "IsReconciled", "type": "STRING"},
    ],
    id_key="BankTransactionID",
)
