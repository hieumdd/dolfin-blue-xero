from typing import Any

from xero.xero_controller import xero_controller
from tasks import tasks_service


def main(request) -> dict[str, Any]:
    body: dict[str, Any] = request.get_json()

    print(body)

    result = (
        xero_controller(body)
        if "table" in body
        else tasks_service.create_tasks_service(body)
    )

    print(result)

    return result
