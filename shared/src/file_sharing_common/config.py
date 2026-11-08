"""Configuration validation, relative paths and atomic JSON persistence."""

from __future__ import annotations

import json
import os
import tempfile
import tomllib
from pathlib import Path


def read_section(path: Path, section: str, allowed: set[str], *, required: bool = False) -> dict:
    if not path.is_file() and not required:
        return {}
    with path.open('rb') as stream:
        document = tomllib.load(stream)
    if set(document) != {section} or not isinstance(document[section], dict):
        raise ValueError(f'Expected a [{section}] configuration section in {path}')
    values = document[section]
    unknown = set(values) - allowed
    if unknown:
        raise ValueError(f'Unknown {section} settings: {", ".join(sorted(unknown))}')
    return values


def port_number(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise ValueError('Port must be an integer between 1 and 65535')
    return value


def host_name(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or any(c.isspace() for c in value):
        raise ValueError('Server address must be a non-empty hostname or IP address')
    return value.strip()


def relative_path(value: object, config_path: Path) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError('Directory setting must be a non-empty path')
    path = Path(value).expanduser()
    return (path if path.is_absolute() else config_path.parent / path).resolve()


def write_json(path: Path, value: dict) -> None:
    """Replace state atomically and keep identity tokens private on POSIX."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=path.stem + '-', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, indent=2)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)
