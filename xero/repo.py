from typing import Any, Callable
import os
from datetime import datetime

from authlib.integrations.requests_client import OAuth2Session

BASE_URL = "https://api.xero.com/api.xro/2.0/"


def get_client():
    return OAuth2Session(
        client_id=os.getenv("XERO_CLIENT_ID"),
        client_secret=os.getenv("XERO_CLIENT_SECRET"),
        token_endpoint="https://identity.xero.com/connect/token",
    )


def get_listing(
    uri: str,
    params: dict[str, Any],
    res_fn: Callable[[dict[str, Any]], list[dict[str, Any]]],
):
    def _get(start: datetime):
        def __get(client: OAuth2Session, page: int = 1):
            with client.get(
                f"{BASE_URL}/{uri}", params=params, headers={"If-Modified-Since": start}
            ) as r:
                res = r.json()
            data = res_fn(res)
            return data if not data else __get(client, page + 1)

        with get_client() as client:
            return __get(client)

    return _get
