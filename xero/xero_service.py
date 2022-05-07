from typing import Union, Optional
from datetime import datetime

from compose import compose

from xero.pipeline.interface import Pipeline
from xero.repo import get_listing
from db.bigquery import get_last_timestamp, load


def _get_timeframe_service(pipeline: Pipeline):
    def _svc(start: Optional[str]) -> datetime:
        return (
            get_last_timestamp(pipeline.name, pipeline.cursor_key)
            if not start
            else datetime.strptime(start, "%Y-%m-%d")
        )
    return _svc


def pipeline_service(
    pipeline: Pipeline,
    start: Optional[str],
) -> dict[str, Union[str, int]]:
    return compose(
        lambda x: {
            "table": pipeline.name,
            "start": start,
            "output_rows": x,
        },
        load(
            pipeline.name,
            pipeline.schema,
            pipeline.id_key,
            pipeline.cursor_key,
        ),
        pipeline.transform,
        get_listing(pipeline.uri, pipeline.params, pipeline.res_fn),
        _get_timeframe_service(pipeline),
    )(start)
