from xero.pipeline.interface import Pipeline
from xero.pipeline.headers import dimension

pipeline = Pipeline(
    name="Accounts",
    headers_fn=dimension,
    uri="api.xro/2.0/Accounts",
    res_fn=lambda x: x["Accounts"],
    paging=False,
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
    id_key=None,
)
