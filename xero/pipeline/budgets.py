from xero.pipeline.interface import Pipeline
from xero.pipeline.headers import dimension
from xero.pipeline.utils import parse_timestamp

pipeline = Pipeline(
    name="Budgets",
    headers_fn=dimension,
    uri="api.xro/2.0/Budgets",
    res_fn=lambda x: x["Budgets"],
    paging=False,
    transform=lambda rows: [
        {
            "BudgetID": row.get("BudgetID"),
            "Type": row.get("Type"),
            "Description": row.get("Description"),
            "UpdatedDateUTC": parse_timestamp(row.get("UpdatedDateUTC")),
            "BudgetLines": [
                {
                    "AccountID": line.get("AccountID"),
                    "AccountCode": line.get("AccountCode"),
                    "BudgetBalances": [
                        {
                            "Period": balance.get("Period"),
                            "Amount": balance.get("Amount"),
                            "Notes": balance.get("Notes"),
                        }
                        for balance in line["BudgetLines"]
                    ]
                    if line.get("BudgetBalances")
                    else [],
                }
                for line in row["BudgetLines"]
            ]
            if row.get("BudgetLines")
            else [],
        }
        for row in rows
    ],
    schema=[
        {"name": "BudgetID", "type": "STRING"},
        {"name": "Type", "type": "STRING"},
        {"name": "Description", "type": "STRING"},
        {"name": "UpdatedDateUTC", "type": "TIMESTAMP"},
        {
            "name": "BudgetLines",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "AccountID", "type": "STRING"},
                {"name": "AccountCode", "type": "STRING"},
                {
                    "name": "BudgetBalances",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {"name": "Period", "type": "STRING"},
                        {"name": "Amount", "type": "STRING"},
                        {"name": "Notes", "type": "STRING"},
                    ],
                },
            ],
        },
    ],
    id_key=None,
)
