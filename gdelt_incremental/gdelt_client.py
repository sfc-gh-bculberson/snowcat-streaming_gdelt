"""GDELT incremental file discovery and download.

GDELT publishes a new events/mentions/gkg trio every 15 minutes at a fixed
naming convention (``YYYYMMDDHHMMSS.<suffix>``):

    http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.export.CSV.zip

Progress is watermark-driven: each window is ``last_watermark + 15 minutes``.
After that window is ingested the watermark is set to that window's timestamp.
We do **not** consult ``lastupdate.txt`` or ``masterfilelist.txt`` — URLs are
constructed from the timestamp, and upstream 404s mean that table/window is
absent.

A wall-clock ceiling (floored UTC grid minus a small publish lag) only prevents
racing ahead of windows that cannot exist yet. Catch-up after an outage is
successive ``+15 minutes`` steps, capped by ``MAX_CATCHUP_WINDOWS``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

from gdelt_incremental import config
from gdelt_incremental.schemas import FILENAME_SUFFIXES, TABLE_ORDER, TableKind

logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"
WINDOW = timedelta(minutes=15)


class GdeltDownloadError(Exception):
    """Base class for GDELT file download failures."""


class GdeltFileNotFound(GdeltDownloadError):
    """Raised when GDELT returns 404 for a file URL (that table/window is
    legitimately absent -- GDELT occasionally skips a table for one window)."""

    def __init__(self, url: str) -> None:
        self.url = url
        super().__init__(f"404 Not Found: {url}")


@dataclass(frozen=True)
class GdeltFile:
    table: TableKind
    timestamp: str
    url: str
    filename: str


def parse_timestamp(ts: str) -> datetime:
    return datetime.strptime(ts, TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)


def format_timestamp(dt: datetime) -> str:
    return dt.strftime(TIMESTAMP_FORMAT)


def _filename_from_url(url: str) -> str:
    return url.rsplit("/", 1)[-1]


def urls_for_timestamp(ts: str) -> Dict[TableKind, GdeltFile]:
    """Construct the predictable per-table URLs for a 15-minute timestamp."""
    files: Dict[TableKind, GdeltFile] = {}
    for table in TABLE_ORDER:
        filename = f"{ts}.{FILENAME_SUFFIXES[table]}"
        url = f"{config.GDELT_BASE_URL}/{filename}"
        files[table] = GdeltFile(table=table, timestamp=ts, url=url, filename=filename)
    return files


def floor_to_window(dt: datetime) -> datetime:
    """Floor a timezone-aware datetime to the preceding 15-minute UTC grid point."""
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    utc = dt.astimezone(timezone.utc)
    minute = (utc.minute // 15) * 15
    return utc.replace(minute=minute, second=0, microsecond=0)


def ingest_ceiling(
    now: Optional[datetime] = None,
    lag_windows: int = config.GDELT_LATEST_LAG_WINDOWS,
) -> str:
    """Newest window timestamp we will attempt (wall-clock floor minus lag).

    Only used to avoid requesting windows that cannot have been published yet.
    """
    if lag_windows < 0:
        raise ValueError("lag_windows must be >= 0")
    current = floor_to_window(now or datetime.now(timezone.utc))
    return format_timestamp(current - WINDOW * lag_windows)


def next_window_after(last_watermark: str) -> str:
    """The next ingest window: last watermark + 15 minutes."""
    return format_timestamp(parse_timestamp(last_watermark) + WINDOW)


def pending_timestamps(
    last_watermark: Optional[str],
    ceiling: str,
    max_windows: int = config.MAX_CATCHUP_WINDOWS,
) -> List[str]:
    """Windows to ingest this run, oldest first.

    With a watermark: ``watermark+15m``, ``watermark+30m``, … while each is
    ``<= ceiling``, capped at ``max_windows``. After each window is ingested the
    watermark is set to that window's timestamp.

    With no watermark (cold start): only the single most recent eligible
    window (``ceiling``) — i.e. within the last ~15 minutes, never a backfill.
    """
    ceiling_dt = parse_timestamp(ceiling)
    if last_watermark is None:
        return [ceiling]

    windows: List[str] = []
    cursor = parse_timestamp(last_watermark) + WINDOW
    while cursor <= ceiling_dt and len(windows) < max_windows:
        windows.append(format_timestamp(cursor))
        cursor += WINDOW

    if cursor <= ceiling_dt:
        logger.warning(
            "Capped catch-up at %d windows; watermark is %s, ceiling is %s. "
            "Remaining gap will be closed over subsequent runs.",
            max_windows,
            last_watermark,
            ceiling,
        )
    return windows


def download(url: str, data_dir: Path = config.DATA_DIR) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    filename = _filename_from_url(url)
    local = data_dir / filename
    part = local.with_name(f"{local.name}.part")

    try:
        with requests.get(url, stream=True, timeout=180) as response:
            if response.status_code == 404:
                raise GdeltFileNotFound(url)
            response.raise_for_status()
            with part.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        os.replace(part, local)
    except GdeltFileNotFound:
        raise
    except Exception:
        try:
            part.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return local
