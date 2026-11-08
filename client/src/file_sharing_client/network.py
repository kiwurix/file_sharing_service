"""Async relay client, streaming transfers and reconnect handling."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import queue
import ssl
import threading
import time
import uuid
import zipfile
from collections import deque
from collections.abc import Callable
from pathlib import Path

from file_sharing_common.logging import LOG_BACKUPS
from file_sharing_common.protocol import CHUNK_SIZE, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, read_frame, send_frame
from .files import available_path
from .i18n import translate

LOG_UPLOAD_CHUNK = 256 * 1024

class NetworkClient:
    def __init__(self, config: dict, events: queue.Queue, logger: logging.Logger, log_dir: Path,
                 *, save_state: Callable[[dict], None]):
        self.config = config
        self.save_state = save_state
        self.events = events
        self.log = logger
        self.log_dir = log_dir
        self.loop: asyncio.AbstractEventLoop | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.write_lock: asyncio.Lock | None = None
        self.data_writer: asyncio.StreamWriter | None = None
        self.data_write_lock: asyncio.Lock | None = None
        self.data_reader_task: asyncio.Task | None = None
        self.pending_paths: dict[str, Path] = {}
        self.file_queue: deque[tuple[str, Path]] = deque()
        self.active_transfer: str | None = None
        self.downloads: dict[str, dict] = {}
        self.authenticated = False
        self.restored_outgoing = False
        self.restored_incoming: set[str] = set()
        self.log_offsets: dict[str, int] = {}
        self.duplicate_session = False
        self.capabilities: set[str] = set()
        self.thread = threading.Thread(target=self._thread_main, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _thread_main(self) -> None:
        asyncio.run(self._run())

    def command(self, message: dict) -> None:
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(self._command(message), self.loop)

    async def send(self, message: dict, payload: bytes = b"") -> None:
        if not self.writer or not self.write_lock:
            raise ConnectionError("not connected")
        async with self.write_lock:
            await send_frame(self.writer, message, payload)

    async def send_data(self, message: dict, payload: bytes = b"") -> None:
        if not self.data_writer or not self.data_write_lock:
            raise ConnectionError("data channel is not connected")
        async with self.data_write_lock:
            await send_frame(self.data_writer, message, payload)

    async def _open_connection(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Open either channel using the same TLS and certificate-pin policy."""
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        reader, writer = await asyncio.wait_for(asyncio.open_connection(
            self.config["server"], int(self.config["port"]), ssl=context,
            server_hostname=self.config["server"], limit=CHUNK_SIZE + 65536), timeout=10)
        ssl_object = writer.get_extra_info("ssl_object")
        fingerprint = hashlib.sha256(ssl_object.getpeercert(binary_form=True)).hexdigest()
        pinned = self.config.get("server_fingerprint")
        if pinned and pinned != fingerprint:
            self.log.error("Server certificate pin mismatch")
            writer.close()
            await writer.wait_closed()
            raise ssl.SSLError("server certificate changed; refusing the connection")
        if not pinned:
            self.config["server_fingerprint"] = fingerprint
            self.save_state(self.config)
            self.log.info("Server certificate pinned on first connection")
        return reader, writer

    async def _connect_data_channel(self) -> None:
        reader, writer = await self._open_connection()
        try:
            await send_frame(writer, {"type": "data_hello", "token": self.config.get("token")})
            message, _ = await asyncio.wait_for(read_frame(reader), timeout=10)
            if message.get("type") != "data_welcome":
                raise ConnectionError(message.get("message", "data channel was rejected"))
        except BaseException:
            writer.close()
            await writer.wait_closed()
            raise
        self.data_writer = writer
        self.data_write_lock = asyncio.Lock()
        self.data_reader_task = asyncio.create_task(self._data_receive_loop(reader))

    async def _data_receive_loop(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                message, payload = await read_frame(reader)
                await self._handle(message, payload)
        except asyncio.CancelledError:
            raise
        except (asyncio.IncompleteReadError, ConnectionError, OSError, ssl.SSLError) as exc:
            self.log.warning("Data channel unavailable: %s", exc)
            if self.writer:
                self.writer.close()

    async def _run(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.write_lock = asyncio.Lock()
        retry_delay = 2
        while True:
            heartbeat_task = None
            try:
                self.log.info("Connecting to relay: address=%s port=%s", self.config["server"], self.config["port"])
                reader, self.writer = await self._open_connection()
                await self.send({"type": "hello", "token": self.config.get("token"), "name": self.config["name"]})
                heartbeat_task = asyncio.create_task(self._heartbeat())
                while True:
                    timeout = 10 if not self.authenticated else HEARTBEAT_TIMEOUT + 2 if "heartbeat" in self.capabilities else None
                    if timeout:
                        message, payload = await asyncio.wait_for(read_frame(reader), timeout=timeout)
                    else:
                        message, payload = await read_frame(reader)
                    await self._handle(message, payload)
                    if message.get("type") == "welcome":
                        retry_delay = 2
            except Exception as exc:
                retry_delay = 15 if self.duplicate_session else min(10, retry_delay)
                self.events.put({"type": "network", "online": False, "message": str(exc), "retry_in": retry_delay,
                                 "duplicate": self.duplicate_session})
                self.log.warning("Relay connection unavailable: %s", exc)
            finally:
                self.authenticated = False
                if self.data_reader_task:
                    self.data_reader_task.cancel()
                    try:
                        await self.data_reader_task
                    except (asyncio.CancelledError, ConnectionError, OSError):
                        pass
                self.data_reader_task = None
                if self.data_writer:
                    self.data_writer.close()
                self.data_writer = None
                self.data_write_lock = None
                if heartbeat_task:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except (asyncio.CancelledError, ConnectionError, OSError):
                        pass
                if self.writer:
                    self.writer.close()
                self.writer = None
            await asyncio.sleep(retry_delay)
            retry_delay = min(10, retry_delay + 2)
            self.duplicate_session = False

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if not self.authenticated:
                continue
            if "heartbeat" in self.capabilities:
                await self.send({"type": "ping", "sent_at": time.monotonic()})
            if "client_logs" in self.capabilities:
                try:
                    await self._send_log_delta()
                except (OSError, ConnectionError) as exc:
                    self.log.warning("Client log synchronization delayed: %s", exc)

    async def _send_log_delta(self) -> None:
        budget = LOG_UPLOAD_CHUNK
        paths = [self.log_dir / f"client.log.{index}" for index in range(LOG_BACKUPS, 0, -1)] + [self.log_dir / "client.log"]
        for path in paths:
            if budget <= 0:
                break
            if not path.exists():
                continue
            key = path.name
            size = path.stat().st_size
            offset = self.log_offsets.get(key, 0)
            if size < offset:
                offset = 0
            if size == offset:
                continue
            amount = min(budget, size - offset)
            with path.open("rb") as stream:
                stream.seek(offset)
                payload = stream.read(amount)
            if payload:
                await self.send({"type": "client_log", "source": key}, payload)
                self.log_offsets[key] = offset + len(payload)
                budget -= len(payload)

    async def _command(self, message: dict) -> None:
        kind = message.pop("type")
        try:
            if kind == "queue_files":
                for raw in message["paths"]:
                    path = Path(raw)
                    if path.is_file() and path.suffix.lower() == ".zip" and zipfile.is_zipfile(path):
                        self.file_queue.append((message["recipient"], path))
                self.log.info("Files queued: recipient=%s count=%d", message["recipient"], len(message["paths"]))
                await self._offer_next()
            elif kind == "accept":
                transfer_id = message["transfer_id"]
                destination = available_path(message["folder"], message["filename"])
                partial = destination.with_suffix(destination.suffix + ".part")
                stream = partial.open("wb")
                self.downloads[transfer_id] = {"destination": destination, "stream": stream, "digest": hashlib.sha256(),
                                               "size": int(message["size"]), "received": 0,
                                               "filename": message["filename"], "last_progress": 0.0}
                await self.send({"type": "accept", "transfer_id": transfer_id})
            elif kind in {"reject", "add_connection"}:
                await self.send({"type": kind, **message})
            elif kind == "reconnect":
                if self.writer:
                    self.writer.close()
        except Exception as exc:
            self.log.exception("Client command failed: command=%s", kind)
            if kind == "queue_files":
                self.pending_paths.clear()
                self.active_transfer = None
                self.events.put({"type": "sending_status", "key": "connection_changed"})
                if self.writer:
                    self.writer.close()
                return
            if kind == "accept" and message.get("transfer_id"):
                self._discard_download(message["transfer_id"])
            self.events.put({"type": "error", "message": str(exc)})

    async def _offer_next(self) -> None:
        if self.active_transfer or not self.file_queue or not self.writer:
            return
        recipient, path = self.file_queue[0]
        request_id = uuid.uuid4().hex
        self.pending_paths[request_id] = path
        self.active_transfer = request_id
        self.events.put({"type": "transfer_progress", "direction": "outgoing", "filename": path.name,
                         "uploaded": 0, "downloaded": 0, "size": path.stat().st_size})
        await self.send({"type": "offer", "request_id": request_id, "recipient": recipient,
                         "filename": path.name, "size": path.stat().st_size})
        self.log.info("Transfer offered: recipient=%s size=%d", recipient, path.stat().st_size)
        default_user = translate(self.config.get("language", "en"), "user")
        recipient_name = next((item.get("name", default_user) for item in self.config.get("connections", [])
                               if item.get("code") == recipient), default_user)
        self.events.put({"type": "sending_status", "key": "waiting_accept",
                         "values": {"recipient": recipient_name, "filename": path.name}})

    async def _advance_queue(self) -> None:
        if self.file_queue:
            self.file_queue.popleft()
        self.active_transfer = None
        await self._offer_next()

    async def _upload(self, transfer_id: str, path: Path) -> None:
        try:
            size = path.stat().st_size
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                while block := stream.read(CHUNK_SIZE):
                    digest.update(block)
                    await self.send_data({"type": "chunk", "transfer_id": transfer_id}, block)
            await self.send_data({"type": "finish", "transfer_id": transfer_id, "sha256": digest.hexdigest()})
            self.log.info("Stream sent to relay: id=%s size=%d", transfer_id[:8], size)
        except Exception:
            self.log.exception("Upload failed: id=%s", transfer_id[:8])
            self.pending_paths.pop(transfer_id, None)
            self.active_transfer = None
            self.events.put({"type": "sending_status", "key": "upload_interrupted"})
            if self.writer:
                self.writer.close()

    async def _handle(self, message: dict, payload: bytes) -> None:
        kind = message.get("type")
        if kind == "welcome":
            self.config.update(token=message["token"], code=message["code"], name=message["name"])
            self.save_state(self.config)
            self.authenticated = True
            self.capabilities = set(message.get("capabilities", []))
            await self._connect_data_channel()
            self.restored_outgoing = False
            self.restored_incoming = set()
            self.events.put({"type": "network", "online": True, "verified": "heartbeat" in self.capabilities})
            self.log.info("Relay session authenticated: code=%s", message["code"])
            if "state_sync" not in self.capabilities:
                await self._offer_next()
        elif kind == "pong":
            latency = max(0, round((time.monotonic() - float(message.get("sent_at", time.monotonic()))) * 1000))
            connections = message.get("connections", self.config["connections"])
            changed = connections != self.config["connections"]
            self.config["connections"] = connections
            if changed:
                self.save_state(self.config)
            self.events.put({"type": "health", "latency_ms": latency})
            if changed:
                self.events.put({"type": "connections", "items": self.config["connections"]})
        elif kind == "connections":
            self.config["connections"] = message["items"]
            self.save_state(self.config)
        elif kind == "connection_added":
            items = [item for item in self.config["connections"] if item["code"] != message["user"]["code"]]
            items.append(message["user"]); self.config["connections"] = items; self.save_state(self.config)
        elif kind == "presence":
            for item in self.config["connections"]:
                if item["code"] == message["code"]:
                    item["online"] = message["online"]
                    if message.get("name"):
                        item["name"] = message["name"]
            self.save_state(self.config)
        elif kind == "offer_created":
            path = self.pending_paths.pop(message["request_id"])
            self.pending_paths[message["transfer_id"]] = path
            self.active_transfer = message["transfer_id"]
            self.restored_outgoing = True
        elif kind == "offer_restored":
            self.restored_outgoing = True
            self.active_transfer = message["transfer_id"]
            if self.file_queue:
                self.pending_paths.clear()
                self.pending_paths[message["transfer_id"]] = self.file_queue[0][1]
                self.events.put({"type": "sending_status", "key": "waiting_receiver"})
            else:
                await self.send({"type": "cancel", "transfer_id": message["transfer_id"]})
                self.active_transfer = None
        elif kind == "sync_complete":
            for transfer_id in list(self.downloads):
                if transfer_id not in self.restored_incoming:
                    self._discard_download(transfer_id)
            if self.active_transfer and not self.restored_outgoing:
                self.pending_paths.clear()
                self.active_transfer = None
            await self._offer_next()
        elif kind == "accepted":
            path = self.pending_paths.get(message["transfer_id"])
            if path:
                asyncio.create_task(self._upload(message["transfer_id"], path))
        elif kind in {"rejected", "upload_complete"}:
            self.pending_paths.pop(message["transfer_id"], None)
            self.events.put(message)
            await self._advance_queue()
            return
        elif kind == "download_chunk":
            target = self.downloads.get(message["transfer_id"])
            if target:
                target["stream"].write(payload); target["digest"].update(payload); target["received"] += len(payload)
                now = time.monotonic()
                if now - target["last_progress"] >= 0.1 or target["received"] == target["size"]:
                    target["last_progress"] = now
                    self.events.put({"type": "transfer_progress", "direction": "incoming",
                                     "filename": target["filename"], "uploaded": target["received"],
                                     "downloaded": target["received"], "size": target["size"]})
            return
        elif kind == "download_complete":
            target = self.downloads.pop(message["transfer_id"], None)
            if target:
                destination, stream, digest = target["destination"], target["stream"], target["digest"]
                stream.close()
                partial = destination.with_suffix(destination.suffix + ".part")
                if target["received"] != target["size"] or digest.hexdigest() != message["sha256"]:
                    partial.unlink(missing_ok=True)
                    self.events.put({"type": "error", "message": "Downloaded file failed its integrity check"})
                elif not zipfile.is_zipfile(partial):
                    partial.unlink(missing_ok=True)
                    self.events.put({"type": "error", "message": "Downloaded file is not a valid ZIP archive"})
                else:
                    os.replace(partial, destination)
                    await self.send({"type": "received", "transfer_id": message["transfer_id"]})
                    self.log.info("Download verified and saved: id=%s size=%d", message["transfer_id"][:8], target["size"])
                    self.events.put({"type": "received_file", "path": str(destination)})
            return
        elif kind == "transfer_cancelled":
            if message["transfer_id"] in self.pending_paths:
                self.pending_paths.pop(message["transfer_id"], None)
                self.active_transfer = None
                self.events.put({"type": "sending_status", "key": "transfer_retry"})
                if self.writer:
                    self.writer.close()
                return
            self._discard_download(message["transfer_id"])
            self.events.put({"type": "sending_status", "key": "transfer_waiting_sender"})
            return
        elif kind == "error" and message.get("code") == "duplicate_session":
            self.duplicate_session = True
            self.events.put({"type": "error", "message": "duplicate session"})
            if self.writer:
                self.writer.close()
            return
        elif kind == "error" and message.get("code") == "request_rejected":
            operation = message.get("operation")
            if operation == "offer":
                request_id = message.get("request_id")
                self.pending_paths.pop(request_id, None)
                self.active_transfer = None
                if self.file_queue:
                    self.file_queue.popleft()
                await self._offer_next()
            elif operation == "accept" and message.get("transfer_id"):
                self._discard_download(message["transfer_id"])
            self.events.put(message)
            return
        self.events.put(message)

    def _discard_download(self, transfer_id: str) -> None:
        target = self.downloads.pop(transfer_id, None)
        if not target:
            return
        target["stream"].close()
        target["destination"].with_suffix(target["destination"].suffix + ".part").unlink(missing_ok=True)
