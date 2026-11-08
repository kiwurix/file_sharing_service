import asyncio
import json
import hashlib
import io
import os
import queue
import ssl
import tempfile
import unittest
import uuid
import zipfile
import logging
from pathlib import Path
from unittest import mock

from file_sharing_common.protocol import read_frame, send_frame
from file_sharing_server.relay import RelayServer
from file_sharing_server.tls import ensure_certificate
from file_sharing_server.config import ServerSettings
from file_sharing_client.config import ClientSettings, StateStore
from file_sharing_client.display import calculate_display_metrics
from file_sharing_client.i18n import STRINGS, friendly_error, translate
from file_sharing_client.network import NetworkClient
from file_sharing_common.logging import configure_logging
from file_sharing_client.single_instance import SingleInstanceGuard


class CoreTests(unittest.TestCase):
    def test_single_instance_guard_rejects_second_copy(self):
        if os.name != "nt":
            self.skipTest("Windows named mutex test")
        name = f"Local\\FileSharingTest-{uuid.uuid4().hex}"
        first = SingleInstanceGuard(name)
        second = SingleInstanceGuard(name)
        try:
            self.assertTrue(first.is_primary)
            self.assertFalse(second.is_primary)
        finally:
            second.close(); first.close()

    def test_server_restart_removes_all_non_offered_transfers(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            relay = RelayServer(root)
            relay.state["transfers"] = {
                "uploading": {"status": "streaming"},
                "ready": {"status": "ready"},
            }
            relay.save()
            restarted = RelayServer(root)
            self.assertNotIn("uploading", restarted.state["transfers"])
            self.assertNotIn("ready", restarted.state["transfers"])

    def test_rotating_log_is_timestamped_and_idempotent(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "logs" / "test.log"
            logger = configure_logging("file_sharing.test", path)
            same_logger = configure_logging("file_sharing.test", path)
            self.assertIs(logger, same_logger)
            logger.info("transfer completed")
            contents = path.read_text("utf-8")
            self.assertIn("INFO", contents)
            self.assertIn("transfer completed", contents)
            self.assertEqual(1, sum(isinstance(handler, logging.handlers.RotatingFileHandler)
                                    for handler in logger.handlers))

    def test_local_server_defaults(self):
        settings = ClientSettings()
        self.assertEqual("localhost", settings.server)
        self.assertEqual(52007, settings.port)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = root / "client.toml"
            config.write_text('[client]\nserver="relay.example.test"\nport=51000\nstate_dir="./private"\n', 'utf-8')
            configured = ClientSettings.load(config)
            self.assertEqual("relay.example.test", configured.server)
            self.assertEqual(51000, configured.port)
            self.assertEqual(root / "private", configured.state_dir)
            config.write_text('[client]\nport=70000\n', 'utf-8')
            with self.assertRaises(ValueError):
                ClientSettings.load(config)
            config.write_text('[client]\nserver_typo="localhost"\n', 'utf-8')
            with self.assertRaises(ValueError):
                ClientSettings.load(config)
            with self.assertRaises(FileNotFoundError):
                ClientSettings.load(root / "missing.toml")
            config.write_text('[server]\nhost="localhost"\nport=51001\ndata_dir="./buffer"\n', 'utf-8')
            server_settings = ServerSettings.load(config)
            self.assertEqual(root / "buffer", server_settings.data_dir)
            self.assertEqual(51001, server_settings.port)

    def test_saved_relay_mode_distinguishes_default_from_manual_address(self):
        with tempfile.TemporaryDirectory() as folder:
            config_path = Path(folder) / "connections.json"
            config_path.write_text(json.dumps({
                "server": "localhost",
                "port": 52007,
                "use_default_server": False,
                "token": "saved-test-identity", "server_fingerprint": "saved-test-pin",
            }), "utf-8")
            store = StateStore(ClientSettings(server="relay.example.test", state_dir=Path(folder), configured=True))
            config = store.load()
            store.save(config)
            self.assertEqual(config, store.load())
        self.assertFalse(config["use_default_server"])
        self.assertEqual("localhost", config["server"])
        self.assertEqual("saved-test-identity", config["token"])
        self.assertEqual("saved-test-pin", config["server_fingerprint"])

    def test_legacy_default_address_uses_masked_default_mode(self):
        with tempfile.TemporaryDirectory() as folder:
            config_path = Path(folder) / "connections.json"
            config_path.write_text(json.dumps({"server": "localhost", "port": 52007}), "utf-8")
            config = StateStore(ClientSettings(state_dir=Path(folder))).load()
        self.assertTrue(config["use_default_server"])
        self.assertEqual("en", config["language"])
        with tempfile.TemporaryDirectory() as folder:
            store = StateStore(ClientSettings(server="relay.example.test", state_dir=Path(folder), configured=True))
            store.save({"server": "localhost", "port": 52007, "use_default_server": True,
                        "token": "saved-test-identity", "server_fingerprint": "saved-test-pin"})
            changed = store.load()
            self.assertEqual("relay.example.test", changed["server"])
            self.assertIsNone(changed["server_fingerprint"])
            self.assertEqual("saved-test-identity", changed["token"])

    def test_saved_language_is_loaded(self):
        with tempfile.TemporaryDirectory() as folder:
            config_path = Path(folder) / "connections.json"
            config_path.write_text(json.dumps({"language": "ru"}), "utf-8")
            config = StateStore(ClientSettings(state_dir=Path(folder))).load()
        self.assertEqual("ru", config["language"])

    def test_technical_errors_are_user_friendly(self):
        self.assertIn("server cannot be reached", friendly_error("not connected").lower())
        self.assertIn("no user was found", friendly_error("invalid connection code").lower())
        self.assertIn("disconnected", friendly_error("receiver disconnected during transfer").lower())
        self.assertIn("сервер", friendly_error("not connected", "ru").lower())

    def test_localizations_have_matching_keys(self):
        self.assertEqual(set(STRINGS["en"]), set(STRINGS["ru"]))
        self.assertEqual("Выберите контакт", translate("ru", "choose_connection"))
        self.assertEqual("Choose a connection", translate("unsupported", "choose_connection"))

    def test_display_metrics_adapt_and_remain_bounded(self):
        laptop = calculate_display_metrics(1366, 768, 1.0)
        full_hd = calculate_display_metrics(1920, 1080, 1.0)
        ultra_hd = calculate_display_metrics(3840, 2160, 2.0)
        self.assertLess(laptop["width"], full_hd["width"])
        self.assertLess(laptop["height"], full_hd["height"])
        self.assertLessEqual(ultra_hd["width"], 2560)
        self.assertLessEqual(ultra_hd["height"], 1720)
        self.assertEqual(2.0, ultra_hd["dpi_scale"])

    def test_identity_is_stable_and_codes_are_unique(self):
        with tempfile.TemporaryDirectory() as folder:
            relay = RelayServer(Path(folder))
            first_token, first = relay.allocate_user(None, "One")
            second_token, second = relay.allocate_user(None, "Two")
            same_token, same = relay.allocate_user(first_token, "Renamed")
            self.assertNotEqual(first["code"], second["code"])
            self.assertEqual(first_token, same_token)
            self.assertEqual("Renamed", same["name"])

    def test_identity_code_and_connections_survive_server_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            relay = RelayServer(root)
            first_token, first = relay.allocate_user(None, "One")
            second_token, second = relay.allocate_user(None, "Two")
            relay.state["connections"][first_token].append(second_token)
            relay.state["connections"][second_token].append(first_token)
            relay.save()

            restarted = RelayServer(root)
            same_token, same_user = restarted.allocate_user(first_token, "One")
            self.assertEqual(first_token, same_token)
            self.assertEqual(first["code"], same_user["code"])
            self.assertEqual([second_token], restarted.user_connections(first_token))

    def test_certificate_is_generated_and_reused(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            cert, key = ensure_certificate(root)
            original = cert.read_bytes()
            self.assertTrue(key.exists())
            if os.name != "nt":
                self.assertEqual(0o600, key.stat().st_mode & 0o777)
            self.assertEqual((cert, key), ensure_certificate(root))
            self.assertEqual(original, cert.read_bytes())

    def test_zip_validation(self):
        with tempfile.TemporaryDirectory() as folder:
            archive = Path(folder) / "valid.zip"
            with zipfile.ZipFile(archive, "w") as bundle:
                bundle.writestr("hello.txt", "hello")
            self.assertTrue(zipfile.is_zipfile(archive))


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_log_delta_uploads_without_tokens(self):
        with tempfile.TemporaryDirectory() as folder:
            log_dir = Path(folder) / "logs"
            log_dir.mkdir()
            (log_dir / "client.log").write_text("diagnostic line\n", "utf-8")
            logger = configure_logging("file_sharing.client-test", log_dir / "local-test.log")
            network = NetworkClient({}, queue.Queue(), logger, log_dir, save_state=mock.Mock())
            sent = []

            async def capture(message, payload=b""):
                sent.append((message, payload))

            network.send = capture
            await network._send_log_delta()
            uploaded = b"".join(payload for message, payload in sent if message["type"] == "client_log")
            self.assertIn(b"diagnostic line", uploaded)
            self.assertNotIn(b"token", uploaded.lower())

    async def test_network_progress_is_throttled(self):
        with tempfile.TemporaryDirectory() as folder:
            relay = RelayServer(Path(folder))
            relay.state["transfers"]["transfer"] = {
                "sender": "one", "recipient": "two", "filename": "file.zip", "size": 4,
                "status": "streaming", "received_bytes": 0}
            messages = []

            async def capture(token, message, payload=b""):
                messages.append((token, message))
                return True

            relay.tell = capture
            relay.tell_data = capture
            with mock.patch("file_sharing_server.relay.time.monotonic", side_effect=[10.0, 10.2]):
                await relay.chunk("one", "transfer", b"a")
                await relay.chunk("one", "transfer", b"b")
            chunks = [message for _, message in messages if message["type"] == "download_chunk"]
            progress = [message for _, message in messages if message["type"] == "transfer_progress"]
            self.assertEqual(2, len(chunks))
            self.assertEqual(1, len(progress))

    async def test_frame_round_trip(self):
        received = asyncio.Future()

        async def handler(reader, writer):
            received.set_result(await read_frame(reader))
            writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        await send_frame(writer, {"type": "chunk", "n": 7}, b"payload")
        message, payload = await asyncio.wait_for(received, 2)
        self.assertEqual({"type": "chunk", "n": 7}, message)
        self.assertEqual(b"payload", payload)
        writer.close(); await writer.wait_closed()
        server.close(); await server.wait_closed()

    async def test_two_way_relay_transfer_lifecycle(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            relay = RelayServer(root)
            cert, key = ensure_certificate(root)
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.load_cert_chain(cert, key)
            server = await asyncio.start_server(relay.handle, "127.0.0.1", 0, ssl=server_context)
            port = server.sockets[0].getsockname()[1]
            client_context = ssl.create_default_context()
            client_context.check_hostname = False
            client_context.verify_mode = ssl.CERT_NONE

            network_config = {"server": "127.0.0.1", "port": port, "server_fingerprint": None}
            network = NetworkClient(network_config, queue.Queue(), relay.log, root / "probe-logs",
                                    save_state=mock.Mock())
            _, probe = await network._open_connection()
            self.assertEqual(hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest(),
                             network_config["server_fingerprint"])
            probe.close()
            await probe.wait_closed()
            network_config["server_fingerprint"] = "wrong-test-pin"
            with self.assertRaises(ssl.SSLError):
                await network._open_connection()

            async def connect(name):
                reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=client_context)
                await send_frame(writer, {"type": "hello", "name": name})
                welcome, _ = await read_frame(reader)
                await read_frame(reader)  # initial connection list
                await read_frame(reader)  # sync complete
                return reader, writer, welcome

            async def receive_type(reader, expected):
                while True:
                    message, payload = await read_frame(reader)
                    if message["type"] == expected:
                        return message, payload

            one_r, one_w, one = await connect("One")
            two_r, two_w, two = await connect("Two")

            async def connect_data(token):
                reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=client_context)
                await send_frame(writer, {"type": "data_hello", "token": token})
                welcome, _ = await read_frame(reader)
                self.assertEqual("data_welcome", welcome["type"])
                return reader, writer

            one_data_r, one_data_w = await connect_data(one["token"])
            two_data_r, two_data_w = await connect_data(two["token"])
            await send_frame(one_w, {"type": "add_connection", "code": two["code"]})
            await receive_type(one_r, "connection_added"); await receive_type(two_r, "connection_added")

            await send_frame(one_w, {"type": "ping", "sent_at": 123.0})
            pong, _ = await receive_type(one_r, "pong")
            self.assertEqual(123.0, pong["sent_at"])
            self.assertEqual(two["code"], pong["connections"][0]["code"])
            self.assertTrue(pong["connections"][0]["online"])

            await send_frame(one_w, {"type": "client_log", "source": "client.log"}, b"client diagnostic line\n")
            await send_frame(one_w, {"type": "ping", "sent_at": 124.0})
            await receive_type(one_r, "pong")
            uploaded_log = root / "client_logs" / f"client-{one['code']}.log"
            self.assertIn("client diagnostic line", uploaded_log.read_text("utf-8"))

            duplicate_r, duplicate_w = await asyncio.open_connection("127.0.0.1", port, ssl=client_context)
            await send_frame(duplicate_w, {"type": "hello", "token": one["token"], "name": "Duplicate"})
            duplicate_error, _ = await read_frame(duplicate_r)
            self.assertEqual("duplicate_session", duplicate_error["code"])
            duplicate_w.close(); await duplicate_w.wait_closed()

            data_stream = io.BytesIO()
            with zipfile.ZipFile(data_stream, "w") as archive:
                archive.writestr("payload.txt", "encrypted relay test")
            data = data_stream.getvalue()
            digest = hashlib.sha256(data).hexdigest()
            await send_frame(one_w, {"type": "offer", "request_id": "r1", "recipient": two["code"],
                                     "filename": "payload.zip", "size": len(data)})
            created, _ = await receive_type(one_r, "offer_created")
            offered, _ = await receive_type(two_r, "incoming_offer")
            transfer_id = created["transfer_id"]
            self.assertEqual(transfer_id, offered["transfer_id"])
            await send_frame(two_w, {"type": "accept", "transfer_id": transfer_id})
            accepted, _ = await receive_type(one_r, "accepted")
            self.assertEqual("accepted", accepted["type"])
            await send_frame(one_data_w, {"type": "chunk", "transfer_id": transfer_id}, data)
            chunk_message, downloaded = await receive_type(two_data_r, "download_chunk")
            self.assertEqual("download_chunk", chunk_message["type"])
            self.assertEqual(data, downloaded)
            upload_progress, _ = await receive_type(one_r, "transfer_progress")
            self.assertEqual(len(data), upload_progress["uploaded"])
            self.assertEqual(len(data), upload_progress["downloaded"])
            await send_frame(one_data_w, {"type": "finish", "transfer_id": transfer_id, "sha256": digest})
            done, _ = await receive_type(two_data_r, "download_complete")
            complete, _ = await receive_type(one_r, "upload_complete")
            self.assertEqual(digest, done["sha256"])
            self.assertEqual("upload_complete", complete["type"])
            await send_frame(two_w, {"type": "received", "transfer_id": transfer_id})
            delivered, _ = await receive_type(one_r, "delivered")
            self.assertEqual("delivered", delivered["type"])
            self.assertFalse((root / "files").exists())
            one_data_w.close(); two_data_w.close()
            await one_data_w.wait_closed(); await two_data_w.wait_closed()
            one_w.close(); two_w.close()
            await one_w.wait_closed(); await two_w.wait_closed()
            server.close(); await server.wait_closed()


if __name__ == "__main__":
    unittest.main()
