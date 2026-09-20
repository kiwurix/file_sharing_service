"""Small framed protocol shared by the client and relay server."""

from __future__ import annotations

import asyncio
import json
import struct

MAX_HEADER = 64 * 1024
CHUNK_SIZE = 1024 * 1024
HEARTBEAT_INTERVAL = 3
HEARTBEAT_TIMEOUT = 10


async def send_frame(writer: asyncio.StreamWriter, message: dict, payload: bytes = b"") -> None:
    header = dict(message)
    header["payload_size"] = len(payload)
    encoded = json.dumps(header, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_HEADER:
        raise ValueError("protocol header is too large")
    writer.write(struct.pack("!I", len(encoded)))
    writer.write(encoded)
    if payload:
        writer.write(payload)
    await writer.drain()


async def read_frame(reader: asyncio.StreamReader) -> tuple[dict, bytes]:
    raw_size = await reader.readexactly(4)
    header_size = struct.unpack("!I", raw_size)[0]
    if not 1 <= header_size <= MAX_HEADER:
        raise ValueError("invalid protocol header size")
    message = json.loads((await reader.readexactly(header_size)).decode("utf-8"))
    payload_size = int(message.pop("payload_size", 0))
    if not 0 <= payload_size <= CHUNK_SIZE:
        raise ValueError("invalid payload size")
    payload = await reader.readexactly(payload_size) if payload_size else b""
    return message, payload
