"""Spill directories are owned by streams and processes, never by DuckDB."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)
# The leading dot is outside the stream NAME grammar. Separate owners even
# when broker and maintenance bases are the same as the storage root.
STREAMS = Path(".spill") / "streams"
MAINTAINERS = Path(".spill") / "maintainers"


def remove(directory: Path) -> None:
    try:
        if directory.is_dir() and not directory.is_symlink():
            shutil.rmtree(directory)
        else:
            directory.unlink(missing_ok=True)

    except OSError:
        logger.exception("could not remove DuckDB spill directory %s", directory)


def prepare(directory: Path, *, empty: bool = True) -> None:
    # Failure is deferred to a query that actually needs spill: an unavailable
    # disk must not prevent appends or small replays from being served.
    try:
        directory.mkdir(parents=True, exist_ok=True)
        if empty:
            for child in directory.iterdir():
                remove(child)

    except OSError:
        logger.exception("could not create DuckDB spill directory %s", directory)
