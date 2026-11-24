"""Spill directories are owned by streams and processes, never by DuckDB."""

from __future__ import annotations

import logging
import shutil
from collections.abc import Iterable
from pathlib import Path

logger = logging.getLogger(__name__)
# The leading dot is outside the stream NAME grammar. Separate owners even
# when broker and maintenance bases are the same as the storage root.
STREAMS = Path(".spill") / "streams"
MAINTAINERS = Path(".spill") / "maintainers"


def validate(directory: Path, storage_roots: Iterable[Path]) -> None:
    """Refuse a cleanup directory that owns durable storage, before any writes."""
    _refuse_symlink(directory)
    resolved = directory.resolve()
    for storage in storage_roots:
        root = storage.resolve()
        contains = root.is_relative_to(resolved)
        # samefile follows the filesystem's case rules, including on volumes
        # whose case sensitivity differs from the platform's Path flavour.
        if not contains and resolved.exists():
            contains = any(
                p.exists() and resolved.samefile(p) for p in (root, *root.parents)
            )

        if contains:
            raise ValueError(
                f"DuckDB spill directory {directory} equals or contains storage root {storage}"
            )


def remove(directory: Path) -> None:
    try:
        if directory.is_dir() and not directory.is_symlink():
            shutil.rmtree(directory)
        else:
            directory.unlink(missing_ok=True)

    except OSError:
        logger.exception("could not remove DuckDB spill directory %s", directory)


def prepare(directory: Path, *, empty: bool = True) -> None:
    _refuse_symlink(directory)

    # Failure is deferred to a query that actually needs spill: an unavailable
    # disk must not prevent appends or small replays from being served.
    try:
        directory.mkdir(parents=True, exist_ok=True)
        if empty:
            for child in directory.iterdir():
                remove(child)

    except OSError:
        logger.exception("could not create DuckDB spill directory %s", directory)


def _refuse_symlink(directory: Path) -> None:
    if directory.is_symlink():
        raise ValueError(f"DuckDB spill directory {directory} must not be a symlink")
