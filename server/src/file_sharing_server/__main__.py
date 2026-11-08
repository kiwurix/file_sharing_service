"""Relay command-line entry point."""

import argparse
import asyncio
import ssl
from pathlib import Path

from file_sharing_common.protocol import CHUNK_SIZE
from file_sharing_server.config import ServerSettings
from file_sharing_server.relay import RelayServer
from file_sharing_server.tls import ensure_certificate


async def run(settings: ServerSettings) -> None:
    relay = RelayServer(settings.data_dir)
    cert, key = ensure_certificate(relay.root)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(cert, key)
    server = await asyncio.start_server(relay.handle, settings.host, settings.port,
                                        ssl=context, limit=CHUNK_SIZE + 65536)
    relay.log.info('TLS relay listening: host=%s port=%d buffer=%s', settings.host, settings.port, relay.root)
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description='File Sharing TLS relay')
    parser.add_argument('--config', type=Path, required=True, help='Local TOML configuration file')
    args = parser.parse_args()
    try:
        settings = ServerSettings.load(args.config)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    asyncio.run(run(settings))


if __name__ == '__main__':
    main()
