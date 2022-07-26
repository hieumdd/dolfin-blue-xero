from xero.pipeline import pipelines
from xero import xero_service


def xero_controller(body: dict[str, str]):
    return xero_service.pipeline_service(
        pipelines[body.get("table", "")],
        body.get("start"),
    )
