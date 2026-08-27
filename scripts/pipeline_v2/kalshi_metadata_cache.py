"""Immutable page cache and append-only manifest support."""

from __future__ import annotations

import hashlib
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any, Callable, Mapping

from scripts.pipeline_v2.kalshi_metadata_planner import EndpointSegment, deterministic_page_filename


class CacheError(RuntimeError):
    pass


class ImmutableConflict(CacheError):
    pass


class SensitiveResponseError(CacheError):
    def __init__(self, field_paths: list[str]) -> None:
        self.field_paths = tuple(sorted(field_paths))
        super().__init__(
            "sensitive response fields rejected at: " + ", ".join(self.field_paths)
        )


class ResourceLimitError(CacheError):
    """Raised before a publication would violate a configured storage limit."""


class StorageBudget:
    """Check a namespace byte ceiling and filesystem free-space floor."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_bytes: int,
        min_free_bytes: int,
        disk_usage: Callable[[Path], Any] = shutil.disk_usage,
    ) -> None:
        if max_bytes <= 0 or min_free_bytes < 0:
            raise ValueError("storage limits must be positive/nonnegative")
        self.root = Path(root)
        self.max_bytes = int(max_bytes)
        self.min_free_bytes = int(min_free_bytes)
        self.disk_usage = disk_usage

    def used_bytes(self) -> int:
        if not self.root.exists():
            return 0
        return sum(
            path.stat().st_size
            for path in self.root.rglob("*")
            if path.is_file()
        )

    def snapshot(self) -> dict[str, int]:
        probe = self.root
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
        free = int(self.disk_usage(probe).free)
        used = self.used_bytes()
        return {
            "used_bytes": used,
            "max_bytes": self.max_bytes,
            "remaining_budget_bytes": max(0, self.max_bytes - used),
            "free_bytes": free,
            "min_free_bytes": self.min_free_bytes,
            "free_space_margin_bytes": free - self.min_free_bytes,
        }

    def check_publication(self, destination: str | Path, content: bytes) -> None:
        path = Path(destination)
        additional = 0 if path.exists() else len(content)
        self.check_additional(additional)

    def check_additional(self, additional: int) -> None:
        if additional < 0:
            raise ValueError("additional byte count must be nonnegative")
        state = self.snapshot()
        if state["used_bytes"] + additional > self.max_bytes:
            raise ResourceLimitError(
                "raw-data budget would be exceeded: "
                f"used={state['used_bytes']} additional={additional} "
                f"maximum={self.max_bytes}"
            )
        if state["free_bytes"] - additional < self.min_free_bytes:
            raise ResourceLimitError(
                "minimum free-space guard would be crossed: "
                f"free={state['free_bytes']} additional={additional} "
                f"minimum={self.min_free_bytes}"
            )


def canonical_sensitive_key(key: Any) -> str:
    """Casefold a key and remove every non-alphanumeric character."""
    return re.sub(r"[^a-z0-9]", "", str(key).casefold())


SENSITIVE_KEYS = frozenset(
    canonical_sensitive_key(key)
    for key in (
        "authorization", "proxyauthorization", "cookie", "setcookie", "apikey",
        "xapikey", "token", "accesstoken", "refreshtoken", "clientsecret",
        "secret", "password", "credential", "credentials",
    )
)


def sensitive_field_paths(value: Any, path: str = "response") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}"
            if canonical_sensitive_key(key) in SENSITIVE_KEYS:
                found.append(child)
            else:
                found.extend(sensitive_field_paths(item, child))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(sensitive_field_paths(item, f"{path}[{index}]"))
    return found


def reject_sensitive_response(response: Any) -> None:
    paths = sensitive_field_paths(response)
    if paths:
        raise SensitiveResponseError(paths)


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def sanitize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): sanitize(item)
            for key, item in value.items()
            if canonical_sensitive_key(key) not in SENSITIVE_KEYS
        }
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    return value


def _fsync_directory(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        pass


def validate_immutable_destination(path: str | Path, content: bytes) -> str:
    destination = Path(path)
    if not destination.exists():
        return "absent"
    if destination.read_bytes() == content:
        return "reused_identical"
    raise ImmutableConflict(f"refusing to overwrite differing immutable file: {destination}")


def publish_immutable_bytes(
    path: str | Path,
    content: bytes,
    *,
    before_install: Callable[[Path, Path], None] | None = None,
    write_bytes: Callable[[int, bytes], int] = os.write,
) -> str:
    """Flush a private same-directory file, then atomically link it into place."""
    destination = Path(path)
    existing = validate_immutable_destination(destination, content)
    if existing == "reused_identical":
        return existing
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.tmp-", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        offset = 0
        while offset < len(content):
            written = write_bytes(descriptor, content[offset:])
            if written <= 0:
                raise OSError("immutable temporary write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        if before_install is not None:
            before_install(temporary, destination)
        try:
            os.link(temporary, destination)
        except FileExistsError:
            return validate_immutable_destination(destination, content)
        _fsync_directory(destination.parent)
        return "published"
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


class MetadataCache:
    def __init__(self, raw_root: str | Path) -> None:
        self.raw_root = Path(raw_root)

    def pages_dir(self, tier: str, cutoff_id: str, month: str | None) -> Path:
        if tier == "historical":
            return self.raw_root / "historical_snapshots" / cutoff_id / "pages"
        if tier == "live" and month:
            return self.raw_root / month / "live_pages"
        raise ValueError("invalid page namespace")

    def page_path(
        self,
        segment: EndpointSegment,
        cutoff_id: str,
        page_number: int,
        request_identifier: str,
        cursor: str | None = None,
    ) -> Path:
        return self.pages_dir(segment.tier, cutoff_id, segment.month) / deterministic_page_filename(
            page_number, cursor, request_identifier
        )

    def load_page(self, path: Path, expected_request: Mapping[str, Any]) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            wrapper = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise CacheError(f"corrupt cache JSON: {path}") from exc
        if wrapper.get("request") != sanitize(dict(expected_request)):
            raise CacheError(f"cached request metadata mismatch: {path}")
        response = wrapper.get("response")
        reject_sensitive_response(response)
        if wrapper.get("response_sha256") != sha256_json(response):
            raise CacheError(f"cached response hash mismatch: {path}")
        return wrapper

    def publish_page(
        self,
        path: Path,
        *,
        request_metadata: Mapping[str, Any],
        response: Any,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        reject_sensitive_response(response)
        wrapper = {
            "schema_version": 1,
            "request": sanitize(dict(request_metadata)),
            "response_sha256": sha256_json(response),
            "response": response,
            "metadata": sanitize(dict(metadata or {})),
        }
        publish_immutable_bytes(path, canonical_json(wrapper) + b"\n")
        return wrapper

    def store_cutoff_snapshot(self, payload: Mapping[str, Any]) -> tuple[str, Path]:
        reject_sensitive_response(payload)
        clean = dict(payload)
        cutoff_id = sha256_json(clean)[:20]
        path = self.raw_root / "cutoff_snapshots" / f"cutoff_{cutoff_id}.json"
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing != clean:
                raise CacheError("cutoff snapshot hash collision")
            return cutoff_id, path
        publish_immutable_bytes(path, canonical_json(clean) + b"\n")
        return cutoff_id, path

    @staticmethod
    def load_cutoff_snapshot(path: str | Path) -> dict[str, Any]:
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except Exception as exc:
            raise CacheError("invalid cutoff snapshot") from exc
        reject_sensitive_response(payload)
        if "response" in payload and isinstance(payload["response"], dict):
            payload = payload["response"]
        reject_sensitive_response(payload)
        if not payload.get("market_settled_ts"):
            raise CacheError("cutoff snapshot lacks market_settled_ts")
        return payload


class CompressedPartitionCache(MetadataCache):
    """Deterministic gzip page cache isolated to one bounded partition."""

    def __init__(
        self,
        raw_root: str | Path,
        *,
        partition_id: str,
        budget: StorageBudget,
    ) -> None:
        super().__init__(raw_root)
        self.partition_id = partition_id
        self.budget = budget

    @staticmethod
    def _compressed(content: bytes) -> bytes:
        return gzip.compress(content, compresslevel=9, mtime=0)

    def pages_dir(self, tier: str, cutoff_id: str, month: str | None) -> Path:
        scope = month if tier == "live" and month else cutoff_id
        return (
            self.raw_root / "partition_pages" / tier / scope /
            self.partition_id / "pages"
        )

    def page_path(
        self,
        segment: EndpointSegment,
        cutoff_id: str,
        page_number: int,
        request_identifier: str,
        cursor: str | None = None,
    ) -> Path:
        plain = deterministic_page_filename(
            page_number, cursor, request_identifier
        )
        return self.pages_dir(segment.tier, cutoff_id, segment.month) / f"{plain}.gz"

    def load_page(self, path: Path, expected_request: Mapping[str, Any]) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            wrapper = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
        except Exception as exc:
            raise CacheError(f"corrupt gzip cache JSON: {path}") from exc
        if wrapper.get("request") != sanitize(dict(expected_request)):
            raise CacheError(f"cached request metadata mismatch: {path}")
        response = wrapper.get("response")
        reject_sensitive_response(response)
        if wrapper.get("response_sha256") != sha256_json(response):
            raise CacheError(f"cached response hash mismatch: {path}")
        return wrapper

    def publish_page(
        self,
        path: Path,
        *,
        request_metadata: Mapping[str, Any],
        response: Any,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        reject_sensitive_response(response)
        wrapper = {
            "schema_version": 2,
            "compression": "gzip",
            "request": sanitize(dict(request_metadata)),
            "response_sha256": sha256_json(response),
            "response": response,
            "metadata": sanitize(dict(metadata or {})),
        }
        content = self._compressed(canonical_json(wrapper) + b"\n")
        self.budget.check_publication(path, content)
        publish_immutable_bytes(path, content)
        return wrapper

    def store_cutoff_snapshot(self, payload: Mapping[str, Any]) -> tuple[str, Path]:
        reject_sensitive_response(payload)
        clean = dict(payload)
        cutoff_id = sha256_json(clean)[:20]
        path = self.raw_root / "cutoff_snapshots" / f"cutoff_{cutoff_id}.json"
        content = canonical_json(clean) + b"\n"
        self.budget.check_publication(path, content)
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing != clean:
                raise CacheError("cutoff snapshot hash collision")
            return cutoff_id, path
        publish_immutable_bytes(path, content)
        return cutoff_id, path


def append_manifest(path: str | Path, record: Mapping[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    clean = sanitize(dict(record))
    with destination.open("a", encoding="utf-8") as handle:
        handle.write(canonical_json(clean).decode("utf-8") + "\n")
        handle.flush()
        os.fsync(handle.fileno())
