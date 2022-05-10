from xero.pipeline.interface import Pipeline
from xero.pipeline.utils import parse_timestamp
from xero.pipeline.headers import timeframe

pipeline = Pipeline(
    name="Accounts",
    headers_fn=timeframe,
    uri="api.xro/2.0/Accounts",
    res_fn=lambda x: x["Accounts"],
    transform=lambda rows: [
        {
            "AccountID": row.get("AccountID"),
            "Code": row.get("Code"),
            "Name": row.get("Name"),
            "Type": row.get("Type"),
            "TaxType": row.get("TaxType"),
            "Description": row.get("Description"),
            "EnablePaymentsToAccount": row.get("EnablePaymentsToAccount"),
        }
        for row in rows
    ],
    schema=[
        {"name": "AccountID", "type": "STRING"},
        {"name": "Code", "type": "STRING"},
        {"name": "Name", "type": "STRING"},
        {"name": "Type", "type": "STRING"},
        {"name": "TaxType", "type": "STRING"},
        {"name": "Description", "type": "STRING"},
        {"name": "EnablePaymentsToAccount", "type": "BOOLEAN"},
    ],
    id_key="AccountID",
)
