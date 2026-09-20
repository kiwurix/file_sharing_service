"""TLS relay and bounded, disk-backed transfer queue."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import secrets
import ssl
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from protocol import CHUNK_SIZE, read_frame, send_frame
from logging_utils import configure_logging

PAIR_LIMIT = 1024 ** 3
MAX_CONNECTIONS = 5


class RelayServer:
    def __init__(self, buffer_dir: Path):
        self.root = buffer_dir.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.files = self.root / "files"
        self.files.mkdir(exist_ok=True)
        self.state_path = self.root / "state.json"
        self.log = configure_logging("file_sharing.server", self.root / "logs" / "server.log", console=True)
        self.state = self._load_state()
        self.online: dict[str, asyncio.StreamWriter] = {}
        self.writer_locks: dict[str, asyncio.Lock] = {}
        self.locks: dict[str, asyncio.Lock] = {}
        self.progress_sent: dict[tuple[str, str], float] = {}
        self.log.info("Relay state loaded: users=%d connections=%d pending_transfers=%d",
                      len(self.state["users"]), sum(len(items) for items in self.state["connections"].values()) // 2,
                      len(self.state["transfers"]))

    def _load_state(self) -> dict:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text("utf-8"))
        return {"users": {}, "codes": {}, "connections": {}, "transfers": {}}

    def save(self) -> None:
        fd, name = tempfile.mkstemp(dir=self.root, prefix="state-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(self.state, stream, indent=2)
            os.replace(name, self.state_path)
        finally:
            if os.path.exists(name):
                os.unlink(name)

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

    async def tell(self, token: str, message: dict, payload: bytes = b"") -> bool:
        writer = self.online.get(token)
        if not writer:
            return False
        try:
            lock = self.writer_locks.setdefault(token, asyncio.Lock())
            async with lock:
                await send_frame(writer, message, payload)
            return True
        except (ConnectionError, asyncio.CancelledError):
            return False

    def pair_usage(self, one: str, two: str) -> int:
        pair = {one, two}
        return sum(int(t["size"]) for t in self.state["transfers"].values()
                   if {t["sender"], t["recipient"]} == pair and t["status"] != "received")

    async def initial_events(self, token: str) -> None:
        await self.tell(token, {"type": "connections", "items": [self.public_user(t) for t in self.user_connections(token)]})
        for transfer_id, item in list(self.state["transfers"].items()):
            if item["recipient"] != token:
                continue
            event = dict(item)
            event.update(type="incoming_offer" if item["status"] == "offered" else "transfer_ready", transfer_id=transfer_id)
            if item["status"] in {"offered", "ready"}:
                await self.tell(token, event)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        token = None
        try:
            hello, _ = await read_frame(reader)
            if hello.get("type") != "hello":
                raise ValueError("hello required")
            token, user = self.allocate_user(hello.get("token"), str(hello.get("name", "User")))
            self.online[token] = writer
            self.writer_locks[token] = asyncio.Lock()
            await self.tell(token, {"type": "welcome", "token": token, "code": user["code"], "name": user["name"]})
            peer = writer.get_extra_info("peername")
            self.log.info("Client connected: code=%s address=%s", user["code"], peer[0] if peer else "unknown")
            await self.initial_events(token)
            for peer in self.user_connections(token):
                await self.tell(peer, {"type": "presence", "code": user["code"], "name": user["name"], "online": True})
            while True:
                message, payload = await read_frame(reader)
                try:
                    await self.dispatch(token, message, payload)
                except (KeyError, TypeError, ValueError) as exc:
                    self.log.warning("Request rejected: code=%s reason=%s", user["code"], exc)
                    await self.tell(token, {"type": "error", "message": str(exc)})
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
                self.online.pop(token, None)
                self.writer_locks.pop(token, None)
                for peer in self.user_connections(token):
                    user = self.state["users"][token]
                    await self.tell(peer, {"type": "presence", "code": user["code"], "name": user["name"], "online": False})
                self.log.info("Client disconnected: code=%s", self.state["users"][token]["code"])
            writer.close()
            await writer.wait_closed()

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
        elif kind == "chunk":
            await self.chunk(token, message["transfer_id"], payload)
        elif kind == "finish":
            await self.finish(token, message["transfer_id"])
        elif kind == "download":
            await self.download(token, message["transfer_id"])
        elif kind == "received":
            await self.received(token, message["transfer_id"])
        else:
            raise ValueError("unknown message type")

    async def offer(self, sender: str, message: dict) -> None:
        recipient = self.state["codes"].get(str(message.get("recipient", "")))
        filename = Path(str(message.get("filename", ""))).name
        size = int(message.get("size", -1))
        if not recipient or recipient not in self.user_connections(sender):
            raise ValueError("recipient is not a connection")
        if Path(filename).suffix.lower() != ".zip" or size < 0:
            raise ValueError("only valid .zip files are supported")
        if self.pair_usage(sender, recipient) + size > PAIR_LIMIT:
            raise ValueError("pair buffer limit (1 GB) exceeded")
        transfer_id = secrets.token_hex(16)
        item = {"sender": sender, "recipient": recipient, "sender_code": self.state["users"][sender]["code"],
                "sender_name": self.state["users"][sender]["name"], "filename": filename, "size": size,
                "sha256": str(message.get("sha256", "")), "status": "offered", "received_bytes": 0}
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
            item["status"] = "uploading"
            self.save()
            self.log.info("Transfer accepted: id=%s", transfer_id[:8])
            await self.tell(item["sender"], {"type": "accepted", "transfer_id": transfer_id})

    async def chunk(self, sender: str, transfer_id: str, payload: bytes) -> None:
        item = self.state["transfers"].get(transfer_id)
        if not item or item["sender"] != sender or item["status"] != "uploading":
            raise ValueError("transfer is not accepting data")
        if item["received_bytes"] + len(payload) > item["size"]:
            raise ValueError("received more data than offered")
        lock = self.locks.setdefault(transfer_id, asyncio.Lock())
        async with lock:
            with (self.files / f"{transfer_id}.part").open("ab") as stream:
                stream.write(payload)
            item["received_bytes"] += len(payload)
        now = time.monotonic()
        progress_key = (transfer_id, "upload")
        if now - self.progress_sent.get(progress_key, 0) >= 1 or item["received_bytes"] == item["size"]:
            self.progress_sent[progress_key] = now
            progress = {"type": "transfer_progress", "transfer_id": transfer_id, "filename": item["filename"],
                        "uploaded": item["received_bytes"], "downloaded": 0, "size": item["size"]}
            await self.tell(sender, {**progress, "direction": "outgoing"})
            await self.tell(item["recipient"], {**progress, "direction": "incoming"})

    async def finish(self, sender: str, transfer_id: str) -> None:
        item = self.state["transfers"].get(transfer_id)
        path = self.files / f"{transfer_id}.part"
        if not item or item["sender"] != sender or item["received_bytes"] != item["size"]:
            raise ValueError("incomplete upload")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(CHUNK_SIZE), b""):
                digest.update(block)
        if digest.hexdigest() != item["sha256"]:
            path.unlink(missing_ok=True)
            self.state["transfers"].pop(transfer_id, None); self.save()
            raise ValueError("file checksum mismatch")
        if not zipfile.is_zipfile(path):
            path.unlink(missing_ok=True)
            self.state["transfers"].pop(transfer_id, None); self.save()
            raise ValueError("the uploaded file is not a valid ZIP archive")
        item["status"] = "ready"; self.save()
        self.log.info("Upload verified and buffered: id=%s size=%d", transfer_id[:8], item["size"])
        event = dict(item); event.update(type="transfer_ready", transfer_id=transfer_id)
        await self.tell(item["recipient"], event)
        await self.tell(sender, {"type": "upload_complete", "transfer_id": transfer_id})

    async def download(self, recipient: str, transfer_id: str) -> None:
        item = self.state["transfers"].get(transfer_id)
        if not item or item["recipient"] != recipient or item["status"] != "ready":
            raise ValueError("download is not ready")
        path = self.files / f"{transfer_id}.part"
        downloaded = 0
        with path.open("rb") as stream:
            while block := stream.read(CHUNK_SIZE):
                await self.tell(recipient, {"type": "download_chunk", "transfer_id": transfer_id}, block)
                downloaded += len(block)
                now = time.monotonic()
                progress_key = (transfer_id, "download")
                if now - self.progress_sent.get(progress_key, 0) >= 1 or downloaded == item["size"]:
                    self.progress_sent[progress_key] = now
                    await self.tell(item["sender"], {"type": "transfer_progress", "direction": "outgoing",
                                    "transfer_id": transfer_id, "filename": item["filename"],
                                    "uploaded": item["size"], "downloaded": downloaded, "size": item["size"]})
        await self.tell(recipient, {"type": "download_complete", "transfer_id": transfer_id, "sha256": item["sha256"]})
        self.log.info("Buffered download sent: id=%s size=%d", transfer_id[:8], downloaded)

    async def received(self, recipient: str, transfer_id: str) -> None:
        item = self.state["transfers"].get(transfer_id)
        if not item or item["recipient"] != recipient:
            return
        (self.files / f"{transfer_id}.part").unlink(missing_ok=True)
        self.progress_sent.pop((transfer_id, "upload"), None)
        self.progress_sent.pop((transfer_id, "download"), None)
        self.state["transfers"].pop(transfer_id, None); self.save()
        self.log.info("Transfer delivered and removed: id=%s", transfer_id[:8])
        await self.tell(item["sender"], {"type": "delivered", "transfer_id": transfer_id})


def ensure_certificate(root: Path) -> tuple[Path, Path]:
    cert, key = root / "server-cert.pem", root / "server-key.pem"
    if cert.exists() and key.exists():
        return cert, key
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError as exc:
        raise RuntimeError("Install dependencies first: pip install -r requirements.txt") from exc
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "File Sharing Relay")])
    now = datetime.now(timezone.utc)
    certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(private.public_key())
                   .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(minutes=1))
                   .not_valid_after(now + timedelta(days=3650)).add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
                   .sign(private, hashes.SHA256()))
    key.write_bytes(private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    key.chmod(0o600)
    cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    return cert, key


async def run(host: str, port: int, buffer_dir: Path) -> None:
    relay = RelayServer(buffer_dir)
    cert, key = ensure_certificate(relay.root)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(cert, key)
    server = await asyncio.start_server(relay.handle, host, port, ssl=context, limit=CHUNK_SIZE + 65536)
    relay.log.info("TLS relay listening: host=%s port=%d buffer=%s", host, port, relay.root)
    print(f"File Sharing relay listening on {host}:{port} (TLS)")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File Sharing TLS relay")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=52007)
    parser.add_argument("--buffer", type=Path, default=Path(__file__).with_name("buffer"))
    args = parser.parse_args()
    asyncio.run(run(args.host, args.port, args.buffer))
