from xero.pipeline.interface import Pipeline
from xero.pipeline.headers import timeframe

pipeline = lambda status: Pipeline(
    name=f"Assets_{status}",
    headers_fn=timeframe,
    uri="assets.xro/1.0/Assets",
    params={
        "status": status
    },
    res_fn=lambda x: x["items"],
    transform=lambda rows: [
        {
            "assetId": row.get("assetId"),
            "assetName": row.get("assetName"),
            "assetNumber": row.get("assetNumber"),
            "purchaseDate": row.get("purchaseDate"),
            "purchasePrice": row.get("purchasePrice"),
            "disposalPrice": row.get("disposalPrice"),
            "assetStatus": row.get("assetStatus"),
            "bookDepreciationSetting": {
                "depreciationMethod": row["bookDepreciationSetting"].get(
                    "depreciationMethod"
                ),
                "averagingMethod": row["bookDepreciationSetting"].get(
                    "averagingMethod"
                ),
                "depreciationRate": row["bookDepreciationSetting"].get(
                    "depreciationRate"
                ),
                "depreciationCalculationMethod": row["bookDepreciationSetting"].get(
                    "depreciationCalculationMethod"
                ),
            }
            if row.get("bookDepreciationSetting")
            else {},
            "bookDepreciationDetail": {
                "currentCapitalGain": row["bookDepreciationDetail"].get(
                    "currentCapitalGain"
                ),
                "currentGainLoss": row["bookDepreciationDetail"].get("currentGainLoss"),
                "depreciationStartDate": row["bookDepreciationDetail"].get(
                    "depreciationStartDate"
                ),
                "costLimit": row["bookDepreciationDetail"].get("costLimit"),
                "residualValue": row["bookDepreciationDetail"].get("residualValue"),
                "priorAccumDepreciationAmount": row["bookDepreciationDetail"].get(
                    "priorAccumDepreciationAmount"
                ),
                "currentAccumDepreciationAmount": row["bookDepreciationDetail"].get(
                    "currentAccumDepreciationAmount"
                ),
            } if row.get("bookDepreciationDetail") else {},
            "canRollback": row.get("canRollback"),
            "accountingBookValue": row.get("accountingBookValue"),
        }
        for row in rows
    ],
    schema=[
        {"name": "assetId", "type": "STRING"},
        {"name": "assetName", "type": "STRING"},
        {"name": "assetNumber", "type": "STRING"},
        {"name": "purchaseDate", "type": "TIMESTAMP"},
        {"name": "purchasePrice", "type": "NUMERIC"},
        {"name": "disposalPrice", "type": "NUMERIC"},
        {"name": "assetStatus", "type": "STRING"},
        {
            "name": "bookDepreciationSetting",
            "type": "record",
            "fields": [
                {"name": "depreciationMethod", "type": "STRING"},
                {"name": "averagingMethod", "type": "STRING"},
                {"name": "depreciationRate", "type": "NUMERIC"},
                {"name": "depreciationCalculationMethod", "type": "STRING"},
            ],
        },
        {
            "name": "bookDepreciationDetail",
            "type": "record",
            "fields": [
                {"name": "currentCapitalGain", "type": "NUMERIC"},
                {"name": "currentGainLoss", "type": "NUMERIC"},
                {"name": "depreciationStartDate", "type": "TIMESTAMP"},
                {"name": "costLimit", "type": "NUMERIC"},
                {"name": "residualValue", "type": "NUMERIC"},
                {"name": "priorAccumDepreciationAmount", "type": "NUMERIC"},
                {"name": "currentAccumDepreciationAmount", "type": "NUMERIC"},
            ],
        },
        {"name": "canRollback", "type": "BOOLEAN"},
        {"name": "accountingBookValue", "type": "NUMERIC"},
    ],
    id_key=None,
)
