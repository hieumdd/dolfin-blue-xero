from typing import Any, Callable
from dataclasses import dataclass, field

Data = list[dict[str, Any]]

@dataclass
class Pipeline:
    name: str
    headers_fn: Callable[[Any], dict[Any, Any]]
    get: Callable[[Any], Data]
    transform: Callable[[Data], Data]
    schema: list[dict[str, Any]]
    id_key: str
    params: dict[Any, Any] = field(default_factory=dict)
    cursor_key: str = "UpdatedDateUTC"
