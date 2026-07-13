from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any


@dataclass
class OpenLinkerError(Exception):
    status_code: int = 0
    code: str = ""
    message: str = ""
    details: Any = None
    request_id: str = ""
    retry_after: timedelta | None = None
    response_body: bytes = b""

    def __str__(self) -> str:
        if not self.code:
            return f"openlinker: request failed with status {self.status_code}"
        return f"openlinker: {self.code}: {self.message}"


Error = OpenLinkerError

