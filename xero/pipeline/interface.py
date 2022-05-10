from typing import Any, Callable, Optional
from dataclasses import dataclass

Data = list[dict[str, Any]]

@dataclass
class Pipeline:
    name: str
    headers_fn: Callable[[Any], dict[Any, Any]]
    get: Callable[[Any], Data]
    transform: Callable[[Data], Data]
    schema: list[dict[str, Any]]
    id_key: Optional[str]
    cursor_key: str = "UpdatedDateUTC"
