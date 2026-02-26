"""Content-addressable hashing utilities for input tracking."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file.

    Args:
        path: Path to the file to hash.

    Returns:
        Hex-encoded SHA-256 digest.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    """Compute SHA-256 hex digest of raw bytes.

    Args:
        data: Bytes to hash.

    Returns:
        Hex-encoded SHA-256 digest.
    """
    return hashlib.sha256(data).hexdigest()


def sha256_dict(d: dict[str, Any]) -> str:
    """Compute SHA-256 of a dict via deterministic JSON serialization.

    Handles non-string keys (e.g. YAML 1.1 boolean coercion of ``on``/``off``)
    by normalizing all dict keys to strings before serialization.
    Values are preserved as-is (type-faithful) — no float→int coercion.

    Args:
        d: Dictionary to hash.

    Returns:
        Hex-encoded SHA-256 digest.
    """
    canonical = json.dumps(_normalize_keys_only(d), sort_keys=True, separators=(",", ":"))
    return sha256_bytes(canonical.encode())


def _normalize_keys_only(obj: Any) -> Any:
    """Recursively normalize dict keys to stripped strings (type-faithful values).

    Used by ``sha256_dict`` and ``compute_plugin_hash_combined`` where the
    original value types must be preserved (e.g. ``1.0`` stays ``1.0``).

    Args:
        obj: Object to normalize.

    Returns:
        Copy with string keys; values unchanged.
    """
    if isinstance(obj, dict):
        return {str(k).strip(): _normalize_keys_only(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize_keys_only(item) for item in obj]
    return obj


def _normalize_keys(obj: Any) -> Any:
    """Recursively normalize dict keys and numeric values for params hashing.

    Used only by ``compute_params_hash`` and ``overlay_hash`` where float→int
    coercion is desirable (YAML ``1.0`` vs ``1`` should hash identically).

    Args:
        obj: Object to normalize.

    Returns:
        Normalized copy with string keys and stable numeric types.
    """
    if isinstance(obj, dict):
        return {str(k).strip(): _normalize_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize_keys(item) for item in obj]
    if isinstance(obj, float):
        # Normalize float-that-is-integer to int for stable serialization
        # (e.g. 1.0 -> 1 so JSON produces "1" not "1.0")
        if obj == obj and obj == int(obj):  # not NaN, exact integer
            return int(obj)
    return obj


def overlay_hash(scenario_name: str, overrides: dict[str, Any]) -> str:
    """Compute a canonical SHA-256 overlay hash for a scenario.

    The hash is deterministic: sorted keys, stable JSON separators,
    scenario name prepended.

    Args:
        scenario_name: Name of the scenario (empty string for default).
        overrides: Parameter overrides dict.

    Returns:
        Hex-encoded SHA-256 digest.
    """
    canonical = json.dumps(
        {"scenario": scenario_name, "overrides": _normalize_keys(overrides)},
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256_bytes(canonical.encode())


def compute_params_hash(effective_params: dict[str, Any]) -> str:
    """Compute a canonical SHA-256 hash for effective parameters.

    The hash is deterministic: sorted keys, stable JSON separators.

    Args:
        effective_params: The resolved parameter dict (spec defaults + overrides).

    Returns:
        Hex-encoded SHA-256 digest.
    """
    canonical = json.dumps(
        {"params": _normalize_keys(effective_params)},
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256_bytes(canonical.encode())


def compute_export_hash(outputs_dir: Path) -> str:
    """Compute a SHA-256 over all exported artifacts in a run outputs directory.

    Hashes scalars.json and all .parquet files in sorted filename order.

    Args:
        outputs_dir: Path to the run's outputs/ directory.

    Returns:
        Hex-encoded SHA-256 digest.
    """
    h = hashlib.sha256()
    files = sorted(outputs_dir.iterdir())
    for f in files:
        if f.is_file() and (f.suffix in (".json", ".parquet")):
            h.update(f.name.encode("utf-8"))
            h.update(f.read_bytes())
    return h.hexdigest()


def compute_plugin_hash_combined(
    engine_version: str,
    plugins_info: dict[str, dict[str, str]],
) -> str:
    """Compute a combined plugin hash from engine version and active plugin info.

    Args:
        engine_version: The fin123 engine version string.
        plugins_info: Dict mapping plugin names to {"version": ..., "sha256": ...}.

    Returns:
        Hex-encoded SHA-256 digest.
    """
    canonical = json.dumps(
        {"engine_version": engine_version, "plugins": _normalize_keys_only(plugins_info)},
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256_bytes(canonical.encode())


def sha256_canonical_json_file(path: Path) -> tuple[str, str]:
    """Hash a lock file, using canonical JSON when the content is valid JSON.

    If the file parses as JSON, re-serializes with ``sort_keys=True`` and
    compact separators so that whitespace and key-order differences do not
    change the hash.  Otherwise falls back to raw byte hashing.

    Args:
        path: Path to the lock file.

    Returns:
        Tuple of (hex_digest, mode) where mode is ``"canonical_json"`` or
        ``"raw_bytes"``.
    """
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
        data = json.loads(text)
        canonical = json.dumps(
            _normalize_keys_only(data), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return sha256_bytes(canonical), "canonical_json"
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
        return sha256_bytes(raw), "raw_bytes"


class InputHashCache:
    """Tracks file hashes with mtime/size-based change detection.

    Avoids re-hashing files that have not changed since the last check.
    Cache is persisted to ``cache/hashes.json`` inside the project directory.
    """

    def __init__(self, cache_path: Path) -> None:
        """Initialize the hash cache.

        Args:
            cache_path: Path to the hashes.json file.
        """
        self.cache_path = cache_path
        self._entries: dict[str, dict[str, Any]] = {}
        if cache_path.exists():
            self._entries = json.loads(cache_path.read_text())

    def get_hash(self, file_path: Path) -> str:
        """Return the SHA-256 hash of *file_path*, using cached value when possible.

        A cached hash is reused when the file size and mtime have not changed.

        Args:
            file_path: Path to the file.

        Returns:
            Hex-encoded SHA-256 digest.
        """
        key = str(file_path.resolve())
        stat = file_path.stat()
        size = stat.st_size
        mtime = stat.st_mtime

        cached = self._entries.get(key)
        if cached and cached["size"] == size and cached["mtime"] == mtime:
            return cached["hash"]

        file_hash = sha256_file(file_path)
        self._entries[key] = {"size": size, "mtime": mtime, "hash": file_hash}
        return file_hash

    def save(self) -> None:
        """Persist the cache to disk."""
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self._entries, indent=2))

    def hashes_for(self, paths: list[Path]) -> dict[str, str]:
        """Compute hashes for multiple files and return a mapping.

        Args:
            paths: List of file paths.

        Returns:
            Dict mapping resolved path strings to their SHA-256 hex digests.
        """
        result = {}
        for p in paths:
            result[str(p.resolve())] = self.get_hash(p)
        self.save()
        return result
