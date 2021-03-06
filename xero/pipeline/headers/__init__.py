from typing import Optional
from datetime import datetime

from xero.pipeline.interface import Pipeline
from db.bigquery import get_last_timestamp


def timeframe(pipeline: Pipeline):
    def _svc(start: Optional[str]) -> dict[str, str]:
        return {}
        # _start = (
        #     get_last_timestamp(pipeline.name, pipeline.cursor_key)
        #     if not start
        #     else datetime.strptime(start, "%Y-%m-%d").isoformat(timespec="seconds")
        # )
        # return {"If-Modified-Since": _start}

    return _svc


def dimension(*args):
    return lambda _: {}
