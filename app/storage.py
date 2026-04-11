import threading
from dataclasses import dataclass
from datetime import datetime


@dataclass
class URLRecord:
    code: str
    original_url: str
    created_at: datetime
    hit_count: int = 0


class InMemoryStore:
    """Thread-safe in-memory URL store. Replaced by Postgres + Redis in milestone 2+."""

    def __init__(self) -> None:
        self._data: dict[str, URLRecord] = {}
        self._lock = threading.Lock()

    def save(self, record: URLRecord) -> None:
        with self._lock:
            self._data[record.code] = record

    def get(self, code: str) -> URLRecord | None:
        with self._lock:
            return self._data.get(code)

    def increment_hits(self, code: str) -> None:
        with self._lock:
            if code in self._data:
                self._data[code].hit_count += 1

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def total(self) -> int:
        with self._lock:
            return len(self._data)


store = InMemoryStore()
