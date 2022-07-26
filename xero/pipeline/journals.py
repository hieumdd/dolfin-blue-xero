from xero.pipeline.interface import Pipeline
from xero.pipeline.utils import parse_timestamp
from xero.pipeline.headers import timeframe


def offset_fn(offset: dict, data: list[dict]):
    return {"offset": data[-1]["JournalNumber"]}


pipeline = Pipeline(
    name="Journals",
    headers_fn=timeframe,
    uri="api.xro/2.0/Journals",
    offset_fn=offset_fn,
    res_fn=lambda x: x["Journals"],
    transform=lambda rows: [
        {
            "JournalID": row.get("JournalID"),
            "JournalDate": parse_timestamp(row.get("JournalDate")),
            "JournalNumber": row.get("JournalNumber"),
            "CreatedDateUTC": parse_timestamp(row.get("CreatedDateUTC")),
            "JournalLines": [
                {
                    "JournalLineID": line.get("JournalLineID"),
                    "AccountID": line.get("AccountID"),
                    "AccountCode": line.get("AccountCode"),
                    "AccountType": line.get("AccountType"),
                    "AccountName": line.get("AccountName"),
                    "Description": line.get("Description"),
                    "NetAmount": line.get("NetAmount"),
                    "GrossAmount": line.get("GrossAmount"),
                    "TaxAmount": line.get("TaxAmount"),
                    "TaxType": line.get("TaxType"),
                    "TaxName": line.get("TaxName"),
                }
                for line in row["JournalLines"]
            ]
            if row.get("JournalLines")
            else {},
        }
        for row in rows
    ],
    schema=[
        {"name": "JournalID", "type": "STRING"},
        {"name": "JournalDate", "type": "TIMESTAMP"},
        {"name": "JournalNumber", "type": "NUMERIC"},
        {"name": "CreatedDateUTC", "type": "TIMESTAMP"},
        {
            "name": "JournalLines",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "JournalLineID", "type": "STRING"},
                {"name": "AccountID", "type": "STRING"},
                {"name": "AccountCode", "type": "STRING"},
                {"name": "AccountType", "type": "STRING"},
                {"name": "AccountName", "type": "STRING"},
                {"name": "Description", "type": "STRING"},
                {"name": "NetAmount", "type": "NUMERIC"},
                {"name": "GrossAmount", "type": "NUMERIC"},
                {"name": "TaxAmount", "type": "NUMERIC"},
                {"name": "TaxType", "type": "STRING"},
                {"name": "TaxName", "type": "STRING"},
            ],
        },
    ],
    id_key="JournalID",
)
