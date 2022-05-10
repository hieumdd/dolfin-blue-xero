from typing import Union, Optional

from compose import compose

from xero.pipeline.interface import Pipeline
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
        pipeline.get,
        pipeline.headers_fn,
    )(start)
