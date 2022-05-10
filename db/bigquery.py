from typing import Any, Optional
from datetime import datetime

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()

DATASET = "Xero"


def get_last_timestamp(table: str, cursor_key: str) -> datetime:
    rows = BQ_CLIENT.query(
        f"SELECT MAX({cursor_key}) AS incre FROM {DATASET}.{table}"
    ).result()
    return [row for row in rows][0]["incre"]


def load(table: str, schema: list[dict[str, Any]], id_key: Optional[str], cursor_key: str):
    def _load(data: list[dict[str, Any]]) -> int:
        if len(data) == 0:
            return 0

        output_rows = (
            BQ_CLIENT.load_table_from_json(  # type: ignore
                data,
                f"{DATASET}.{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_TRUNCATE" if not id_key else "WRITE_APPEND",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        if id_key:
            update(table, id_key, cursor_key)
        return output_rows

    return _load


def update(table: str, id_key: str, cursor_key: str):
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {DATASET}.{table}
    AS
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY {cursor_key} DESC) AS row_num,
        FROM {DATASET}.{table}
    ) WHERE row_num = 1
    """
    ).result()
