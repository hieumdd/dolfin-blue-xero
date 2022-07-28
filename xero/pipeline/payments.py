from xero.pipeline.interface import Pipeline
from xero.pipeline.utils import parse_timestamp
from xero.pipeline.headers import timeframe

pipeline = Pipeline(
    name="Payments",
    headers_fn=timeframe,
    uri="api.xro/2.0/Payments",
    res_fn=lambda x: x["Payments"],
    transform=lambda rows: [
        {
            "PaymentID": row.get("PaymentID"),
            "Date": parse_timestamp(row.get("Date")),
            "BankAmount": row.get("BankAmount"),
            "Amount": row.get("Amount"),
            "CurrencyRate": row.get("CurrencyRate"),
            "PaymentType": row.get("PaymentType"),
            "Status": row.get("Status"),
            "UpdatedDateUTC": parse_timestamp(row.get("UpdatedDateUTC")),
            "HasAccount": row.get("HasAccount"),
            "IsReconciled": row.get("IsReconciled"),
            "Account": {
                "AccountID": row["Account"].get("AccountId"),
            }
            if row.get("Account")
            else {},
        }
        for row in rows
    ],
    schema=[
        {"name": "PaymentID", "type": "STRING"},
        {"name": "Date", "type": "TIMESTAMP"},
        {"name": "BankAmount", "type": "NUMERIC"},
        {"name": "Amount", "type": "NUMERIC"},
        {"name": "CurrencyRate", "type": "NUMERIC"},
        {"name": "PaymentType", "type": "STRING"},
        {"name": "Status", "type": "STRING"},
        {"name": "UpdatedDateUTC", "type": "TIMESTAMP"},
        {"name": "HasAccount", "type": "BOOLEAN"},
        {"name": "IsReconciled", "type": "BOOLEAN"},
        {
            "name": "Account",
            "type": "record",
            "fields": [{"name": "AccountID", "type": "STRING"}],
        },
    ],
    id_key="PaymentID",
)
