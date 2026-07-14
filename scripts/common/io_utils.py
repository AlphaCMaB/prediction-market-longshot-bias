"""Explicit CSV I/O helpers with no import-time filesystem activity."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Mapping, Any


def read_csv(path: str | Path) -> list[dict[str, str]]:
    """Read a CSV file. The filesystem is touched only when called."""
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(
    path: str | Path,
    rows: Iterable[Mapping[str, Any]],
    *,
    fieldnames: Iterable[str] | None = None,
    create_parents: bool = True,
) -> None:
    """Write CSV rows, optionally creating the parent directory when called."""
    destination = Path(path)
    materialized = [dict(row) for row in rows]

    if fieldnames is None:
        names = list(materialized[0]) if materialized else []
    else:
        names = list(fieldnames)

    if not names:
        raise ValueError("fieldnames are required when writing an empty CSV")

    if create_parents:
        destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)
