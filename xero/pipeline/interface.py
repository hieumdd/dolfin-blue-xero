from typing import Any, Callable, Optional
from dataclasses import dataclass, field

from xero.pipeline.offset_fn import page_offset

Data = list[dict[str, Any]]


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
