from typing import Optional
from datetime import datetime, timezone
import re


def parse_timestamp(value: Optional[str]) -> Optional[str]:
    if value is None:
        return value
    else:
        pattern = "(?!\()\d+(?=\+)"
        search = re.search(pattern, value)
        if search:
            timestamp = int(search.group()) / 1000
            return (
                datetime.fromtimestamp(timestamp)
                .replace(tzinfo=timezone.utc)
                .isoformat(timespec="seconds")
            )
        else:
            return None
