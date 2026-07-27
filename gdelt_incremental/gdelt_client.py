"""GDELT incremental file discovery and download.

GDELT publishes a new events/mentions/gkg trio every 15 minutes at a fixed
naming convention (``YYYYMMDDHHMMSS.<suffix>``) and always points
``lastupdate.txt`` at the most recent trio:

    http://data.gdeltproject.org/gdeltv2/lastupdate.txt

Unlike ../streaming_gdelt (which strides across the full masterfilelist for a
one-time bulk/load-test import), this loader only ever needs to know "what's
the latest timestamp" and "what's our watermark", then walks the predictable
15-minute grid between the two -- no need to fetch or parse the full
masterfilelist on every run.
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


def fetch_lastupdate(url: str = config.LASTUPDATE_URL) -> Dict[TableKind, GdeltFile]:
    """Fetch and parse lastupdate.txt -> the latest GdeltFile per table."""
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    files: Dict[TableKind, GdeltFile] = {}
    for line in response.text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        file_url = parts[2]
        filename = _filename_from_url(file_url)
        table = _detect_table(filename)
        if table is None:
            continue
        timestamp = filename.split(".", 1)[0]
        files[table] = GdeltFile(table=table, timestamp=timestamp, url=file_url, filename=filename)
    return files


def _detect_table(filename: str) -> Optional[TableKind]:
    lowered = filename.lower()
    for table, suffix in FILENAME_SUFFIXES.items():
        if lowered.endswith(suffix.lower()):
            return table
    return None


def latest_timestamp(lastupdate_files: Dict[TableKind, GdeltFile]) -> Optional[str]:
    """The trio's shared timestamp (all three files share one window)."""
    timestamps = {f.timestamp for f in lastupdate_files.values()}
    if not timestamps:
        return None
    return max(timestamps)


def pending_timestamps(
    last_watermark: Optional[str],
    latest: str,
    max_windows: int = config.MAX_CATCHUP_WINDOWS,
) -> List[str]:
    """Ordered list of 15-minute-grid timestamps strictly after the watermark
    up to and including ``latest``, capped at ``max_windows`` (oldest first).

    If there is no watermark yet (first-ever run), only ``latest`` is
    returned -- we intentionally don't backfill history on cold start.
    """
    latest_dt = parse_timestamp(latest)
    if last_watermark is None:
        return [latest]

    last_dt = parse_timestamp(last_watermark)
    if last_dt >= latest_dt:
        return []

    windows: List[str] = []
    cursor = last_dt + WINDOW
    while cursor <= latest_dt and len(windows) < max_windows:
        windows.append(format_timestamp(cursor))
        cursor += WINDOW

    if cursor <= latest_dt:
        logger.warning(
            "Capped catch-up at %d windows; watermark is %s, latest is %s. "
            "Remaining gap will be closed over subsequent runs.",
            max_windows,
            last_watermark,
            latest,
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
