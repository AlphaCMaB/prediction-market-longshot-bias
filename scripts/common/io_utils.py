"""Explicit CSV I/O helpers with no import-time filesystem activity."""

from __future__ import annotations

import csv
from contextlib import contextmanager
import gzip
from pathlib import Path
from typing import Any, Generator, Iterable, Iterator, Mapping, TextIO


@contextmanager
def open_text_auto(path: str | Path) -> Generator[TextIO, None, None]:
    """Open UTF-8 text, transparently decoding deterministic gzip inputs."""
    source = Path(path)
    if source.suffix.casefold() == ".gz":
        with gzip.open(source, "rt", newline="", encoding="utf-8-sig") as handle:
            yield handle
    else:
        with source.open(newline="", encoding="utf-8-sig") as handle:
            yield handle


@contextmanager
def open_csv_dict_reader(
    path: str | Path,
) -> Generator[tuple[csv.DictReader, tuple[str, ...]], None, None]:
    """Yield a streaming CSV reader and validated header for plain or gzip CSV."""
    with open_text_auto(path) as handle:
        reader = csv.DictReader(handle)
        fields = tuple(reader.fieldnames or ())
        if not fields:
            raise ValueError(f"CSV has no header: {path}")
        if len(fields) != len(set(fields)):
            raise ValueError(f"CSV has duplicate header columns: {path}")
        yield reader, fields


def iter_csv(path: str | Path) -> Iterator[dict[str, str]]:
    """Stream rows from a plain or gzip CSV without retaining the whole file."""
    with open_csv_dict_reader(path) as (reader, _):
        for row in reader:
            yield dict(row)


def read_csv(path: str | Path) -> list[dict[str, str]]:
    """Read a CSV file. The filesystem is touched only when called."""
    return list(iter_csv(path))


def read_csv_with_header(
    path: str | Path,
) -> tuple[list[dict[str, str]], tuple[str, ...]]:
    """Read rows and the declared schema, including for a header-only CSV."""
    with open_csv_dict_reader(path) as (reader, fields):
        return list(reader), fields


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
