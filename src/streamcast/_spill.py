"""Spill directories are owned by streams and processes, never by DuckDB."""

from __future__ import annotations

import logging
import shutil
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlsplit

from streamcast import _metadata
from streamcast._published import path as published_path

logger = logging.getLogger(__name__)
# The leading dot is outside the stream NAME grammar. Separate owners even
# when broker and maintenance bases are the same as the storage root.
_NAMESPACE = ".spill"
STREAMS = Path(_NAMESPACE) / "streams"
MAINTAINERS = Path(_NAMESPACE) / "maintainers"


def data_paths(home: Path, name: str, published: str | None = None) -> set[Path]:
    data = {home / name}
    if published is not None and urlsplit(published).scheme in ("", "file"):
        data.add(Path(published_path(_metadata.within(published, name))))

    return data


def validate(
    directory: Path, storage_roots: Iterable[Path], data: Iterable[Path] = ()
) -> None:
    """Refuse observable aliases; prepare logs uninspectable spill paths."""
    _refuse_symlink(directory)
    try:
        resolved = directory.resolve()
    except (OSError, RuntimeError):
        return

    for storage in storage_roots:
        try:
            root = storage.resolve()
        except (OSError, RuntimeError):
            continue

        contains = root.is_relative_to(resolved)
        # samefile follows the filesystem's case rules, including on volumes
        # whose case sensitivity differs from the platform's Path flavour.
        try:
            if not contains and resolved.exists():
                contains = any(
                    p.exists() and resolved.samefile(p) for p in (root, *root.parents)
                )

        except OSError:
            pass

        if contains:
            raise ValueError(
                f"DuckDB spill directory {directory} equals or contains storage root {storage}"
            )

    for storage in data:
        try:
            live = storage.resolve()
        except (OSError, RuntimeError):
            continue

        overlaps = resolved.is_relative_to(live) or live.is_relative_to(resolved)
        try:
            if not overlaps:
                overlaps = any(
                    a.exists() and b.exists() and a.samefile(b)
                    for a, b in (
                        *((resolved, p) for p in (live, *live.parents)),
                        *((live, p) for p in resolved.parents),
                    )
                )

        except OSError:
            pass

        if overlaps:
            raise ValueError(
                f"DuckDB spill directory {directory} overlaps storage data {storage}"
            )

    # Scratch inside a root or legacy home is valid, but another stream's log
    # may be there too, outside the caller's explicitly known data directories.
    for parent in (resolved, *resolved.parents):
        try:
            log = _metadata._is_log(parent)
        except OSError:
            continue

        if log:
            raise ValueError(
                f"DuckDB spill directory {directory} is inside storage log {parent}"
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
    try:
        symlink = directory.is_symlink()
    except OSError:
        # prepare logs an inaccessible spill disk; it must not stop serving.
        return

    if symlink:
        raise ValueError(f"DuckDB spill directory {directory} must not be a symlink")
