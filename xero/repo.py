from typing import Any, Callable
import os
from datetime import datetime
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
                "accounting.contacts.read",
                "accounting.budgets.read",
                "accounting.reports.read",
                "accounting.transactions.read",
            ]
        ),
    ) as client:
        token = client.fetch_token(
            "https://identity.xero.com/connect/token",
            grant_type="client_credentials",
        )
    return OAuth2Client(
        **auth_info,
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
    res_fn: Callable[[dict[str, Any]], list[dict[str, Any]]],
):
    def _get(start: datetime):
        def __get(client: OAuth2Client, page: int = 1):
            r = client.get(
                f"{BASE_URL}/{uri}",
                params={**params, "page": page},
                headers={"If-Modified-Since": start.isoformat(timespec="seconds")},
            )
            if r.status_code == 429:
                time.sleep(2)
                return __get(client, page)
            else:
                r.raise_for_status()
                res = r.json()
                data = res_fn(res)
                return data if not data else data + __get(client, page + 1)

        with get_client() as client:
            return __get(client)

    return _get
