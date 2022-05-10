from typing import Any, Callable, Optional
from dataclasses import dataclass, field

Data = list[dict[str, Any]]

@dataclass
class Pipeline:
    name: str
    headers_fn: Callable[[Any], dict[Any, Any]]
    uri: str
    res_fn: Callable[[dict[str, Any]], Data]
    transform: Callable[[Data], Data]
    schema: list[dict[str, Any]]
    id_key: Optional[str]
    params: dict[Any, Any] = field(default_factory=dict)
    paging: bool = True
    cursor_key: str = "UpdatedDateUTC"
