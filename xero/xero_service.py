from typing import Union, Optional

from compose import compose

from xero.pipeline.interface import Pipeline
from xero.repo import get_listing
from db.bigquery import load


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
        get_listing(
            pipeline.uri,
            pipeline.params,
            pipeline.res_fn,
            pipeline.offset_fn,
            pipeline.paging,
        ),
        pipeline.headers_fn(pipeline),
    )(start)
