"""Relay defaults and private desktop identity state."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from file_sharing_common.config import host_name, port_number, read_section, relative_path, write_json
from .i18n import STRINGS

DEFAULT_CONFIG_PATH = Path(__file__).resolve().with_name('config.toml')


def default_state_dir() -> Path:
    return Path(os.environ.get('LOCALAPPDATA', Path.home() / 'AppData' / 'Local')) / 'file_sharing'


@dataclass(frozen=True)
class ClientSettings:
    server: str = 'localhost'
    port: int = 52007
    language: str = 'en'
    state_dir: Path = field(default_factory=default_state_dir)
    configured: bool = False

    @classmethod
    def load(cls, path: Path | None = None) -> ClientSettings:
        config_path = (path if path is not None else DEFAULT_CONFIG_PATH).expanduser().resolve()
        values = read_section(config_path, 'client', {'server', 'port', 'language', 'state_dir'},
                              required=path is not None)
        language = values.get('language', 'en')
        if not isinstance(language, str) or language not in STRINGS:
            raise ValueError('Language must be en or ru')
        return cls(server=host_name(values.get('server', 'localhost')),
                   port=port_number(values.get('port', 52007)), language=language,
                   state_dir=relative_path(values['state_dir'], config_path) if 'state_dir' in values else default_state_dir(),
                   configured=config_path.is_file())


class StateStore:
    """Keep identities compatible with existing connections.json files."""

    def __init__(self, settings: ClientSettings):
        self.settings = settings
        self.path = settings.state_dir / 'connections.json'

    def load(self) -> dict:
        settings = self.settings
        defaults = {'token': None, 'code': '---', 'name': '', 'language': settings.language,
                    'server': settings.server, 'port': settings.port, 'use_default_server': True,
                    'server_fingerprint': None, 'connections': []}
        if self.path.is_file():
            stored = json.loads(self.path.read_text('utf-8'))
            if not isinstance(stored, dict):
                raise ValueError('Client state must be a JSON object')
            defaults.update(stored)
            endpoint = (defaults['server'], defaults['port'])
            configured_endpoint = (settings.server, settings.port)
            if 'use_default_server' not in stored:
                defaults['use_default_server'] = endpoint == configured_endpoint
            # Preserve a saved endpoint when no new local configuration was supplied.
            if settings.configured and defaults['use_default_server']:
                if endpoint != configured_endpoint:
                    defaults['server_fingerprint'] = None
                defaults.update(server=settings.server, port=settings.port)
        if defaults.get('language') not in STRINGS:
            defaults['language'] = settings.language
        host_name(defaults['server'])
        port_number(defaults['port'])
        return defaults

    def save(self, state: dict) -> None:
        write_json(self.path, state)
