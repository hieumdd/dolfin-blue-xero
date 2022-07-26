from typing import Any, Callable, Optional
from dataclasses import dataclass, field

Data = list[dict[str, Any]]


def page_offset(offset: dict, _: list[dict]):
    page = offset.get("page", 1)
    return {"page": page + 1}


@dataclass
class Pipeline:
    name: str
    headers_fn: Callable[[Any], dict[Any, Any]]
    uri: str
    transform: Callable[[Data], Data]
    schema: list[dict[str, Any]]
    id_key: Optional[str]
    res_fn: Callable[[dict[str, Any]], Data]
    offset_fn: Callable[[dict, list[dict]], dict] = page_offset
    params: dict[Any, Any] = field(default_factory=dict)
    paging: bool = True
    cursor_key: str = "UpdatedDateUTC"
