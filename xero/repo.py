from typing import Any, Callable
import os
import time

from authlib.integrations.httpx_client import OAuth2Client

BASE_URL = "https://api.xero.com/"


def get_client():
    auth_info = {
        "client_id": os.getenv("XERO_CLIENT_ID"),
        "client_secret": os.getenv("XERO_CLIENT_SECRET"),
    }
    with OAuth2Client(
        **auth_info,
        scope=" ".join(
            [
                "accounting.transactions.read",
                "accounting.reports.read",
                "accounting.budgets.read",
                "accounting.journals.read",
                "payroll.employees.read",
                "payroll.payruns.read",
                "payroll.payslip.read",
                "payroll.settings.read",
                "payroll.timesheets.read",
                "projects.read",
                "accounting.settings.read",
                "accounting.contacts.read",
                "accounting.attachments.read",
                "assets.read",
            ]
        ),
    ) as client:
        token = client.fetch_token(
            "https://identity.xero.com/connect/token",
            grant_type="client_credentials",
        )
    return OAuth2Client(
        **auth_info,
        base_url=BASE_URL,
        token=token,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        timeout=None,
    )


def get_listing(
    uri: str,
    params: dict[str, Any],
    res_fn: Callable[[dict], list[dict]],
    offset_fn: Callable[[dict, list[dict]], dict],
    paging: bool,
):
    def _get(headers: dict):
        def __get(client: OAuth2Client, offset: dict = {"page": 1}):
            r = client.get(uri, params={**params, **offset})
            if r.status_code == 429:
                time.sleep(2)
                return __get(client, offset)
            else:
                r.raise_for_status()
                res = r.json()
                data = res_fn(res)

                return (
                    data
                    if not data or not paging
                    else data + __get(client, offset_fn(offset, data))
                )

        with get_client() as client:
            client.headers.update(headers)
            return __get(client)

    return _get
