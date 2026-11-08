"""TLS control service and streaming TCP file relay."""

from __future__ import annotations

import asyncio
import json
import secrets
import ssl
import time
from pathlib import Path

from file_sharing_common.config import write_json
from file_sharing_common.protocol import HEARTBEAT_TIMEOUT, read_frame, send_frame
from file_sharing_common.logging import LOG_BACKUPS, LOG_SIZE, configure_logging

MAX_CONNECTIONS = 5

class RelayServer:
    def __init__(self, buffer_dir: Path):
        self.root = buffer_dir.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.client_logs = self.root / "client_logs"
        self.client_logs.mkdir(exist_ok=True)
        self.state_path = self.root / "state.json"
        self.log = configure_logging("file_sharing.server", self.root / "logs" / "server.log", console=True)
        self.state = self._load_state()
        self.online: dict[str, asyncio.StreamWriter] = {}
        self.writer_locks: dict[str, asyncio.Lock] = {}
        self.data_online: dict[str, asyncio.StreamWriter] = {}
        self.data_writer_locks: dict[str, asyncio.Lock] = {}
        self.progress_sent: dict[tuple[str, str], float] = {}
        self._recover_interrupted_transfers()
        self.log.info("Relay state loaded: users=%d connections=%d pending_transfers=%d",
                      len(self.state["users"]), sum(len(items) for items in self.state["connections"].values()) // 2,
                      len(self.state["transfers"]))

    def _recover_interrupted_transfers(self) -> None:
        interrupted = [transfer_id for transfer_id, item in self.state["transfers"].items()
                       if item.get("status") != "offered"]
        for transfer_id in interrupted:
            self.state["transfers"].pop(transfer_id, None)
        if interrupted:
            self.save()
            self.log.warning("Removed %d interrupted transfer(s) during startup recovery", len(interrupted))

    def _load_state(self) -> dict:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text("utf-8"))
        return {"users": {}, "codes": {}, "connections": {}, "transfers": {}}

    def save(self) -> None:
        write_json(self.state_path, self.state)

    def user_connections(self, token: str) -> list[str]:
        return list(self.state["connections"].get(token, []))

    def public_user(self, token: str) -> dict:
        user = self.state["users"][token]
        return {"code": user["code"], "name": user.get("name", "User"), "online": token in self.online}

    def allocate_user(self, token: str | None, name: str) -> tuple[str, dict]:
        if token and token in self.state["users"]:
            self.state["users"][token]["name"] = name[:40] or "User"
            self.save()
            self.log.info("Known client authenticated: code=%s", self.state["users"][token]["code"])
            return token, self.state["users"][token]
        if len(self.state["codes"]) >= 1000:
            raise ValueError("all three-digit codes are in use")
        available = [f"{n:03d}" for n in range(1000) if f"{n:03d}" not in self.state["codes"]]
        code = secrets.choice(available)
        token = secrets.token_urlsafe(32)
        user = {"code": code, "name": name[:40] or "User"}
        self.state["users"][token] = user
        self.state["codes"][code] = token
        self.state["connections"][token] = []
        self.save()
        self.log.info("New client registered: code=%s", code)
        return token, user

    async def _tell(self, token: str, message: dict, payload: bytes, *, data: bool) -> bool:
        writers = self.data_online if data else self.online
        locks = self.data_writer_locks if data else self.writer_locks
        writer = writers.get(token)
        if not writer:
            return False
        try:
            async with locks.setdefault(token, asyncio.Lock()):
                await send_frame(writer, message, payload)
            return True
        except (ConnectionError, OSError, ssl.SSLError, asyncio.CancelledError):
            writer.close()
            return False

    async def tell(self, token: str, message: dict, payload: bytes = b"") -> bool:
        return await self._tell(token, message, payload, data=False)

    async def tell_data(self, token: str, message: dict, payload: bytes = b"") -> bool:
        return await self._tell(token, message, payload, data=True)

    def append_client_logs(self, token: str, source: str, payload: bytes) -> None:
        if not payload:
            return
        code = self.state["users"][token]["code"]
        path = self.client_logs / f"client-{code}.log"
        if path.exists() and path.stat().st_size + len(payload) > LOG_SIZE:
            oldest = path.with_suffix(path.suffix + f".{LOG_BACKUPS}")
            oldest.unlink(missing_ok=True)
            for index in range(LOG_BACKUPS - 1, 0, -1):
                current = path.with_suffix(path.suffix + f".{index}")
                if current.exists():
                    current.replace(path.with_suffix(path.suffix + f".{index + 1}"))
            path.replace(path.with_suffix(path.suffix + ".1"))
        safe_source = "".join(char for char in source if char.isalnum() or char in ".-_")[:40] or "client.log"
        with path.open("ab") as stream:
            stream.write(f"\n--- received {safe_source} ---\n".encode("utf-8"))
            stream.write(payload.replace(b"\x00", b""))

    async def initial_events(self, token: str) -> None:
        await self.tell(token, {"type": "connections", "items": [self.public_user(t) for t in self.user_connections(token)]})
        for transfer_id, item in list(self.state["transfers"].items()):
            if item["sender"] == token:
                if item["status"] == "offered":
                    await self.tell(token, {"type": "offer_restored", "transfer_id": transfer_id,
                                            "filename": item["filename"], "size": item["size"]})
            if item["recipient"] != token:
                continue
            event = dict(item)
            event.update(type="incoming_offer", transfer_id=transfer_id)
            if item["status"] == "offered":
                await self.tell(token, event)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        token = None
        try:
            hello, _ = await read_frame(reader)
            if hello.get("type") == "data_hello":
                await self.handle_data_connection(reader, writer, str(hello.get("token", "")))
                return
            if hello.get("type") != "hello":
                raise ValueError("hello required")
            requested_token = hello.get("token")
            if requested_token and requested_token in self.online:
                self.log.warning("Duplicate client session rejected: code=%s",
                                 self.state["users"].get(requested_token, {}).get("code", "unknown"))
                await send_frame(writer, {"type": "error", "code": "duplicate_session",
                                          "message": "this identity is already connected"})
                return
            token, user = self.allocate_user(requested_token, str(hello.get("name", "User")))
            self.online[token] = writer
            self.writer_locks[token] = asyncio.Lock()
            await self.tell(token, {"type": "welcome", "token": token, "code": user["code"], "name": user["name"],
                                    "protocol_version": 4,
                                    "capabilities": ["heartbeat", "client_logs", "state_sync", "split_data_channel"]})
            peer = writer.get_extra_info("peername")
            self.log.info("Client connected: code=%s address=%s", user["code"], peer[0] if peer else "unknown")
            await self.initial_events(token)
            await self.tell(token, {"type": "sync_complete"})
            for peer in self.user_connections(token):
                await self.tell(peer, {"type": "presence", "code": user["code"], "name": user["name"], "online": True})
            while True:
                message, payload = await asyncio.wait_for(read_frame(reader), timeout=HEARTBEAT_TIMEOUT)
                if self.online.get(token) is not writer:
                    break
                try:
                    await self.dispatch(token, message, payload)
                except (KeyError, TypeError, ValueError) as exc:
                    self.log.warning("Request rejected: code=%s reason=%s", user["code"], exc)
                    await self.tell(token, {"type": "error", "code": "request_rejected", "message": str(exc),
                                            "operation": message.get("type"), "request_id": message.get("request_id"),
                                            "transfer_id": message.get("transfer_id")})
        except asyncio.TimeoutError:
            if token:
                self.log.warning("Heartbeat timed out: code=%s", self.state["users"][token]["code"])
        except (asyncio.IncompleteReadError, ConnectionError, ssl.SSLError):
            pass
        except Exception as exc:
            self.log.exception("Unexpected client-session failure")
            try:
                await send_frame(writer, {"type": "error", "message": str(exc)})
            except Exception:
                pass
        finally:
            if token and self.online.get(token) is writer:
                await self.cancel_streams(token)
                data_writer = self.data_online.pop(token, None)
                self.data_writer_locks.pop(token, None)
                if data_writer:
                    data_writer.close()
                self.online.pop(token, None)
                self.writer_locks.pop(token, None)
                for peer in self.user_connections(token):
                    user = self.state["users"][token]
                    await self.tell(peer, {"type": "presence", "code": user["code"], "name": user["name"], "online": False})
                self.log.info("Client disconnected: code=%s", self.state["users"][token]["code"])
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError, ssl.SSLError):
                pass

    async def handle_data_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                                     token: str) -> None:
        if token not in self.state["users"] or token not in self.online:
            raise ValueError("authenticated control connection required")
        previous = self.data_online.get(token)
        if previous:
            previous.close()
        self.data_online[token] = writer
        self.data_writer_locks[token] = asyncio.Lock()
        await self.tell_data(token, {"type": "data_welcome"})
        try:
            while self.online.get(token):
                message, payload = await read_frame(reader)
                kind = message.get("type")
                if kind == "chunk":
                    await self.chunk(token, message["transfer_id"], payload)
                elif kind == "finish":
                    await self.finish(token, message["transfer_id"], message.get("sha256"))
                else:
                    raise ValueError("unknown data message type")
        finally:
            if self.data_online.get(token) is writer:
                self.data_online.pop(token, None)
                self.data_writer_locks.pop(token, None)

    async def cancel_streams(self, token: str) -> None:
        interrupted = [(transfer_id, item) for transfer_id, item in self.state["transfers"].items()
                       if (token in {item["sender"], item["recipient"]} and item["status"] == "streaming")
                       or (token == item["recipient"] and item["status"] == "awaiting_receipt")]
        for transfer_id, item in interrupted:
            self.state["transfers"].pop(transfer_id, None)
            other = item["recipient"] if token == item["sender"] else item["sender"]
            await self.tell(other, {"type": "transfer_cancelled", "transfer_id": transfer_id})
            self.log.warning("Interrupted stream removed after disconnect: id=%s", transfer_id[:8])
        if interrupted:
            self.save()

    async def dispatch(self, token: str, message: dict, payload: bytes) -> None:
        kind = message.get("type")
        if kind == "add_connection":
            code = str(message.get("code", ""))
            other = self.state["codes"].get(code)
            if not other or other == token:
                raise ValueError("invalid connection code")
            mine, theirs = self.user_connections(token), self.user_connections(other)
            if other not in mine and (len(mine) >= MAX_CONNECTIONS or len(theirs) >= MAX_CONNECTIONS):
                raise ValueError("connection limit reached")
            if other not in mine:
                mine.append(other); theirs.append(token)
                self.state["connections"][token] = mine
                self.state["connections"][other] = theirs
                self.save()
                self.log.info("Connection added: codes=%s,%s", self.state["users"][token]["code"], code)
            await self.tell(token, {"type": "connection_added", "user": self.public_user(other)})
            await self.tell(other, {"type": "connection_added", "user": self.public_user(token)})
        elif kind == "offer":
            await self.offer(token, message)
        elif kind in {"accept", "reject"}:
            await self.decision(token, message["transfer_id"], kind)
        elif kind == "received":
            await self.received(token, message["transfer_id"])
        elif kind == "ping":
            await self.tell(token, {"type": "pong", "sent_at": message.get("sent_at"),
                                    "connections": [self.public_user(peer) for peer in self.user_connections(token)]})
        elif kind == "client_log":
            self.append_client_logs(token, str(message.get("source", "client.log")), payload)
        elif kind == "cancel":
            await self.cancel_transfer(token, message["transfer_id"])
        else:
            raise ValueError("unknown message type")

    async def cancel_transfer(self, sender: str, transfer_id: str) -> None:
        item = self.state["transfers"].get(transfer_id)
        if not item or item["sender"] != sender or item["status"] not in {"offered", "streaming"}:
            return
        self.state["transfers"].pop(transfer_id, None)
        self.save()
        await self.tell(item["recipient"], {"type": "transfer_cancelled", "transfer_id": transfer_id})
        self.log.info("Transfer cancelled by sender: id=%s", transfer_id[:8])

    async def offer(self, sender: str, message: dict) -> None:
        recipient = self.state["codes"].get(str(message.get("recipient", "")))
        filename = Path(str(message.get("filename", ""))).name
        size = int(message.get("size", -1))
        if not recipient or recipient not in self.user_connections(sender):
            raise ValueError("recipient is not a connection")
        if Path(filename).suffix.lower() != ".zip" or size < 0:
            raise ValueError("only valid .zip files are supported")
        transfer_id = secrets.token_hex(16)
        item = {"sender": sender, "recipient": recipient, "sender_code": self.state["users"][sender]["code"],
                "sender_name": self.state["users"][sender]["name"], "filename": filename, "size": size,
                "status": "offered", "received_bytes": 0}
        self.state["transfers"][transfer_id] = item
        self.save()
        self.log.info("Transfer offered: id=%s sender=%s recipient=%s size=%d", transfer_id[:8],
                      item["sender_code"], self.state["users"][recipient]["code"], size)
        await self.tell(sender, {"type": "offer_created", "request_id": message.get("request_id"), "transfer_id": transfer_id})
        event = dict(item); event.update(type="incoming_offer", transfer_id=transfer_id)
        await self.tell(recipient, event)

    async def decision(self, recipient: str, transfer_id: str, decision: str) -> None:
        item = self.state["transfers"].get(transfer_id)
        if not item or item["recipient"] != recipient or item["status"] != "offered":
            raise ValueError("transfer is no longer available")
        if decision == "reject":
            self.state["transfers"].pop(transfer_id)
            self.save()
            self.log.info("Transfer rejected: id=%s", transfer_id[:8])
            await self.tell(item["sender"], {"type": "rejected", "transfer_id": transfer_id, "filename": item["filename"]})
        else:
            if recipient not in self.data_online or item["sender"] not in self.data_online:
                raise ValueError("both clients must be online to start a transfer")
            item["status"] = "streaming"
            self.save()
            self.log.info("Transfer accepted: id=%s", transfer_id[:8])
            await self.tell(item["sender"], {"type": "accepted", "transfer_id": transfer_id})

    async def chunk(self, sender: str, transfer_id: str, payload: bytes) -> None:
        item = self.state["transfers"].get(transfer_id)
        if not item or item["sender"] != sender or item["status"] != "streaming":
            raise ValueError("transfer is not accepting data")
        if item["received_bytes"] + len(payload) > item["size"]:
            raise ValueError("received more data than offered")
        delivered = await self.tell_data(item["recipient"], {"type": "download_chunk", "transfer_id": transfer_id}, payload)
        if not delivered:
            self.state["transfers"].pop(transfer_id, None)
            self.save()
            raise ValueError("receiver disconnected during transfer")
        item["received_bytes"] += len(payload)
        now = time.monotonic()
        progress_key = (transfer_id, "upload")
        if now - self.progress_sent.get(progress_key, 0) >= 1 or item["received_bytes"] == item["size"]:
            self.progress_sent[progress_key] = now
            await self.tell(sender, {"type": "transfer_progress", "direction": "outgoing",
                            "transfer_id": transfer_id, "filename": item["filename"],
                            "uploaded": item["received_bytes"], "downloaded": item["received_bytes"],
                            "size": item["size"]})

    async def finish(self, sender: str, transfer_id: str, sha256: object) -> None:
        item = self.state["transfers"].get(transfer_id)
        if not item or item["sender"] != sender or item["received_bytes"] != item["size"]:
            raise ValueError("incomplete upload")
        digest = str(sha256 or "").lower()
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("invalid file checksum")
        item["sha256"] = digest
        item["status"] = "awaiting_receipt"; self.save()
        if not await self.tell_data(item["recipient"], {"type": "download_complete", "transfer_id": transfer_id,
                                                        "sha256": digest}):
            self.state["transfers"].pop(transfer_id, None); self.save()
            raise ValueError("receiver disconnected during transfer")
        self.log.info("Stream relayed: id=%s size=%d", transfer_id[:8], item["size"])
        await self.tell(sender, {"type": "upload_complete", "transfer_id": transfer_id})

    async def received(self, recipient: str, transfer_id: str) -> None:
        item = self.state["transfers"].get(transfer_id)
        if not item or item["recipient"] != recipient:
            return
        self.progress_sent.pop((transfer_id, "upload"), None)
        self.state["transfers"].pop(transfer_id, None); self.save()
        self.log.info("Transfer delivery acknowledged: id=%s", transfer_id[:8])
        await self.tell(item["sender"], {"type": "delivered", "transfer_id": transfer_id})
