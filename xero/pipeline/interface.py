from typing import Any, Callable
from dataclasses import dataclass

@dataclass
class Pipeline:
    name: str
    uri: str
    res_fn: Callable[[dict[str, Any]], list[dict[str, Any]]]
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    schema: list[dict[str, Any]]
    id_key: str
    params: dict[Any, Any] = {}
    cursor_key: str = "_batched_at"
