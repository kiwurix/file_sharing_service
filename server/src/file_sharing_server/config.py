"""Relay listener settings and persistent data directory."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from file_sharing_common.config import host_name, port_number, read_section, relative_path

DEFAULT_CONFIG_PATH = Path(__file__).resolve().with_name('config.toml')


@dataclass(frozen=True)
class ServerSettings:
    host: str
    port: int
    data_dir: Path

    @classmethod
    def load(cls, path: Path | None = None) -> ServerSettings:
        config_path = (path if path is not None else DEFAULT_CONFIG_PATH).expanduser().resolve()
        values = read_section(config_path, 'server', {'host', 'port', 'data_dir'}, required=path is not None)
        return cls(host=host_name(values.get('host', '0.0.0.0')),
                   port=port_number(values.get('port', 52007)),
                   data_dir=relative_path(values.get('data_dir', './buffer'), config_path))
