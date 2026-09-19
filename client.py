"""Desktop client for the encrypted file-sharing relay."""

from __future__ import annotations

import asyncio
import ctypes
import hashlib
import json
import os
import queue
import ssl
import threading
import uuid
import zipfile
from collections import deque
from pathlib import Path
import tkinter as tk
from tkinter import filedialog
from tkinter import ttk

from protocol import CHUNK_SIZE, read_frame, send_frame

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
except ImportError:
    DND_FILES = None
    TkinterDnD = None

APP_DIR = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "file_sharing"
CONFIG_PATH = APP_DIR / "connections.json"
DEFAULT_SERVER = "localhost"
DEFAULT_PORT = 52007


def enable_high_dpi() -> None:
    """Ask Windows for real monitor pixels instead of a bitmap-scaled window."""
    if os.name != "nt":
        return
    try:
        ctypes.windll.user32.SetProcessDpiAwarenessContext(ctypes.c_void_p(-4))
    except (AttributeError, OSError):
        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(2)
        except (AttributeError, OSError):
            try:
                ctypes.windll.user32.SetProcessDPIAware()
            except (AttributeError, OSError):
                pass


def calculate_display_metrics(screen_width: int, screen_height: int, dpi_scale: float = 1.0) -> dict:
    """Return physical-pixel geometry that preserves logical object size."""
    scale = min(3.0, max(1.0, float(dpi_scale)))
    logical_width, logical_height = screen_width / scale, screen_height / scale
    width = round(min(1280, max(860, logical_width * 0.72)) * scale)
    height = round(min(860, max(560, logical_height * 0.78)) * scale)
    width = min(width, max(round(640 * scale), screen_width - round(40 * scale)))
    height = min(height, max(round(480 * scale), screen_height - round(40 * scale)))
    return {"dpi_scale": scale, "width": width, "height": height}


def friendly_error(message: str) -> str:
    """Translate protocol and socket errors into actionable user-facing text."""
    text = str(message).strip()
    lowered = text.lower()
    translations = (
        (("certificate changed",), "The server's security certificate changed. Verify the server before reconnecting, then reset the saved server address."),
        (("not connected", "connection refused", "actively refused", "winerror 10061"), "The server cannot be reached right now. Check your internet connection and server settings; the app will keep retrying."),
        (("getaddrinfo", "name or service not known", "nodename nor servname"), "The server address could not be found. Check the address in Profile & server."),
        (("timed out", "timeout"), "The server took too long to respond. Check your connection and try again."),
        (("invalid connection code",), "No user was found with that code. Check the three digits and try again."),
        (("connection limit reached",), "This connection cannot be added because one of you already has five connections."),
        (("pair buffer limit",), "This transfer would exceed the 1 GB pending-file limit for this connection."),
        (("only valid .zip", "not a valid zip"), "Only valid ZIP archives can be sent. Create or choose a ZIP file and try again."),
        (("transfer is no longer available",), "This transfer is no longer available. Ask the sender to send it again."),
        (("checksum mismatch", "integrity check"), "The file did not arrive intact. It was removed; please send it again."),
        (("incomplete upload",), "The upload was interrupted before it finished. Please send the file again."),
        (("recipient is not a connection",), "That user is no longer in your connections. Add them again using their code."),
    )
    for needles, friendly in translations:
        if any(needle in lowered for needle in needles):
            return friendly
    return f"Something went wrong. Please try again.\n\nDetails: {text}" if text else "Something went wrong. Please try again."


def load_config() -> dict:
    APP_DIR.mkdir(parents=True, exist_ok=True)
    defaults = {"token": None, "code": "---", "name": "",
                "server": DEFAULT_SERVER, "port": DEFAULT_PORT, "use_default_server": True,
                "server_fingerprint": None, "connections": []}
    if CONFIG_PATH.exists():
        try:
            stored = json.loads(CONFIG_PATH.read_text("utf-8"))
            defaults.update(stored)
            if "use_default_server" not in stored:
                defaults["use_default_server"] = (
                    defaults["server"], defaults["port"]
                ) == (DEFAULT_SERVER, DEFAULT_PORT)
        except (OSError, ValueError):
            pass
    return defaults


def save_config(config: dict) -> None:
    APP_DIR.mkdir(parents=True, exist_ok=True)
    temporary = CONFIG_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(config, indent=2), "utf-8")
    os.replace(temporary, CONFIG_PATH)


def available_path(folder: str | Path, filename: str) -> Path:
    destination = Path(folder) / Path(filename).name
    counter = 1
    while destination.exists() or destination.with_suffix(destination.suffix + ".part").exists():
        destination = Path(folder) / f"{Path(filename).stem} ({counter}){Path(filename).suffix}"
        counter += 1
    return destination


class NetworkClient:
    def __init__(self, config: dict, events: queue.Queue):
        self.config = config
        self.events = events
        self.loop: asyncio.AbstractEventLoop | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.write_lock: asyncio.Lock | None = None
        self.pending_paths: dict[str, Path] = {}
        self.file_queue: deque[tuple[str, Path]] = deque()
        self.active_transfer: str | None = None
        self.downloads: dict[str, dict] = {}
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

    async def _run(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.write_lock = asyncio.Lock()
        while True:
            try:
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                reader, self.writer = await asyncio.open_connection(
                    self.config["server"], int(self.config["port"]), ssl=context,
                    server_hostname=self.config["server"], limit=CHUNK_SIZE + 65536)
                ssl_object = self.writer.get_extra_info("ssl_object")
                fingerprint = hashlib.sha256(ssl_object.getpeercert(binary_form=True)).hexdigest()
                pinned = self.config.get("server_fingerprint")
                if pinned and pinned != fingerprint:
                    raise ssl.SSLError("server certificate changed; refusing the connection")
                if not pinned:
                    self.config["server_fingerprint"] = fingerprint
                    save_config(self.config)
                await self.send({"type": "hello", "token": self.config.get("token"), "name": self.config["name"]})
                self.events.put({"type": "network", "online": True})
                while True:
                    message, payload = await read_frame(reader)
                    await self._handle(message, payload)
            except Exception as exc:
                self.events.put({"type": "network", "online": False, "message": str(exc)})
            finally:
                if self.writer:
                    self.writer.close()
                self.writer = None
            await asyncio.sleep(3)

    async def _command(self, message: dict) -> None:
        kind = message.pop("type")
        try:
            if kind == "queue_files":
                for raw in message["paths"]:
                    path = Path(raw)
                    if path.is_file() and path.suffix.lower() == ".zip" and zipfile.is_zipfile(path):
                        self.file_queue.append((message["recipient"], path))
                await self._offer_next()
            elif kind == "accept":
                transfer_id = message["transfer_id"]
                destination = available_path(message["folder"], message["filename"])
                partial = destination.with_suffix(destination.suffix + ".part")
                stream = partial.open("wb")
                self.downloads[transfer_id] = {"destination": destination, "stream": stream, "digest": hashlib.sha256(),
                                               "size": int(message["size"]), "received": 0, "filename": message["filename"]}
                await self.send({"type": "accept", "transfer_id": transfer_id})
            elif kind in {"reject", "add_connection"}:
                await self.send({"type": kind, **message})
            elif kind == "download":
                await self.send({"type": "download", "transfer_id": message["transfer_id"]})
            elif kind == "reconnect":
                if self.writer:
                    self.writer.close()
        except Exception as exc:
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
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(CHUNK_SIZE), b""):
                digest.update(block)
        await self.send({"type": "offer", "request_id": request_id, "recipient": recipient,
                         "filename": path.name, "size": path.stat().st_size, "sha256": digest.hexdigest()})
        self.events.put({"type": "sending_status", "text": f"Waiting for {recipient} to accept {path.name}"})

    async def _advance_queue(self) -> None:
        if self.file_queue:
            self.file_queue.popleft()
        self.active_transfer = None
        await self._offer_next()

    async def _upload(self, transfer_id: str, path: Path) -> None:
        try:
            uploaded = 0
            size = path.stat().st_size
            with path.open("rb") as stream:
                while block := stream.read(CHUNK_SIZE):
                    await self.send({"type": "chunk", "transfer_id": transfer_id}, block)
                    uploaded += len(block)
                    self.events.put({"type": "transfer_progress", "direction": "outgoing", "filename": path.name,
                                     "uploaded": uploaded, "downloaded": 0, "size": size})
            await self.send({"type": "finish", "transfer_id": transfer_id})
        except Exception as exc:
            self.events.put({"type": "error", "message": f"Upload failed: {exc}"})
            await self._advance_queue()

    async def _handle(self, message: dict, payload: bytes) -> None:
        kind = message.get("type")
        if kind == "welcome":
            self.config.update(token=message["token"], code=message["code"], name=message["name"])
            save_config(self.config)
            await self._offer_next()
        elif kind == "connections":
            self.config["connections"] = message["items"]
            save_config(self.config)
        elif kind == "connection_added":
            items = [item for item in self.config["connections"] if item["code"] != message["user"]["code"]]
            items.append(message["user"]); self.config["connections"] = items; save_config(self.config)
        elif kind == "presence":
            for item in self.config["connections"]:
                if item["code"] == message["code"]:
                    item["online"] = message["online"]
                    if message.get("name"):
                        item["name"] = message["name"]
            save_config(self.config)
        elif kind == "offer_created":
            path = self.pending_paths.pop(message["request_id"])
            self.pending_paths[message["transfer_id"]] = path
            self.active_transfer = message["transfer_id"]
        elif kind == "accepted":
            path = self.pending_paths.get(message["transfer_id"])
            if path:
                asyncio.create_task(self._upload(message["transfer_id"], path))
        elif kind in {"rejected", "upload_complete"}:
            self.pending_paths.pop(message["transfer_id"], None)
            self.events.put(message)
            await self._advance_queue()
            return
        elif kind == "transfer_ready":
            # A reconnect may deliver a completed accepted transfer.
            if message["transfer_id"] not in self.downloads:
                self.events.put(message)
                return
            await self.send({"type": "download", "transfer_id": message["transfer_id"]})
            self.events.put({"type": "transfer_progress", "direction": "incoming", "filename": message["filename"],
                             "uploaded": message["size"], "downloaded": 0, "size": message["size"]})
            return
        elif kind == "download_chunk":
            target = self.downloads.get(message["transfer_id"])
            if target:
                target["stream"].write(payload); target["digest"].update(payload); target["received"] += len(payload)
                self.events.put({"type": "transfer_progress", "direction": "incoming", "filename": target["filename"],
                                 "uploaded": target["size"], "downloaded": target["received"], "size": target["size"]})
            return
        elif kind == "download_complete":
            target = self.downloads.pop(message["transfer_id"], None)
            if target:
                destination, stream, digest = target["destination"], target["stream"], target["digest"]
                stream.close()
                partial = destination.with_suffix(destination.suffix + ".part")
                if digest.hexdigest() != message["sha256"]:
                    partial.unlink(missing_ok=True)
                    self.events.put({"type": "error", "message": "Downloaded file failed its integrity check"})
                else:
                    os.replace(partial, destination)
                    await self.send({"type": "received", "transfer_id": message["transfer_id"]})
                    self.events.put({"type": "received_file", "path": str(destination)})
            return
        self.events.put(message)


class App:
    BG, PANEL, CARD, TEXT, MUTED, ACCENT, GREEN, RED = "#10131a", "#171b24", "#202632", "#f4f6fb", "#929bad", "#6c7cff", "#42d392", "#ff647c"

    def __init__(self):
        enable_high_dpi()
        root_class = TkinterDnD.Tk if TkinterDnD else tk.Tk
        self.root = root_class()
        self.root.title("File Sharing")
        dpi_scale = self.root.winfo_fpixels("1i") / 96.0
        metrics = calculate_display_metrics(self.root.winfo_screenwidth(), self.root.winfo_screenheight(), dpi_scale)
        self.ui_scale = metrics["dpi_scale"]
        x = max(0, (self.root.winfo_screenwidth() - metrics["width"]) // 2)
        y = max(0, (self.root.winfo_screenheight() - metrics["height"]) // 2)
        self.root.geometry(f"{metrics['width']}x{metrics['height']}+{x}+{y}")
        self.root.minsize(min(self._p(780), metrics["width"]), min(self._p(520), metrics["height"]))
        self.root.configure(bg=self.BG)
        self.config = load_config()
        if not str(self.config.get("name", "")).strip():
            self.root.withdraw()
            nickname = self._prompt_nickname()
            if not nickname or not nickname.strip():
                self.root.destroy()
                raise SystemExit("A nickname is required")
            self.config["name"] = nickname.strip()[:24]
            save_config(self.config)
            self.root.deiconify()
        self.events: queue.Queue = queue.Queue()
        self.network = NetworkClient(self.config, self.events)
        self.connection_rows: dict[str, tk.Frame] = {}
        self.selected_code: str | None = None
        self.is_online = False
        self._build()
        self._render_connections()
        self.network.start()
        self.root.after(80, self._poll)

    def _p(self, value):
        """Scale logical pixels to the monitor's physical DPI."""
        if isinstance(value, tuple):
            return tuple(self._p(part) for part in value)
        return round(value * self.ui_scale)

    def _modal(self, title: str, width: int, height: int, parent=None) -> tk.Toplevel:
        parent = parent or self.root
        dialog = tk.Toplevel(parent)
        dialog.title(title)
        dialog.configure(bg=self.PANEL)
        dialog.resizable(False, False)
        if parent is not self.root or self.root.winfo_viewable():
            dialog.transient(parent)
        dialog.update_idletasks()
        pixel_width, pixel_height = self._p(width), self._p(height)
        if parent is self.root and not self.root.winfo_viewable():
            x = (self.root.winfo_screenwidth() - pixel_width) // 2
            y = (self.root.winfo_screenheight() - pixel_height) // 2
        else:
            x = parent.winfo_rootx() + max(0, (parent.winfo_width() - pixel_width) // 2)
            y = parent.winfo_rooty() + max(0, (parent.winfo_height() - pixel_height) // 2)
        dialog.geometry(f"{pixel_width}x{pixel_height}+{max(0, x)}+{max(0, y)}")
        dialog.grab_set()
        dialog.focus_force()
        return dialog

    def _close_modal(self, dialog: tk.Toplevel, parent=None) -> None:
        dialog.grab_release()
        dialog.destroy()
        if parent and parent.winfo_exists():
            parent.grab_set()

    def _alert(self, title: str, message: str, *, tone="error", parent=None) -> None:
        parent = parent or self.root
        dialog = self._modal(title, 440, 230, parent)
        accent = self.RED if tone == "error" else self.ACCENT
        icon = "!" if tone == "error" else "i"
        body = tk.Frame(dialog, bg=self.PANEL); body.pack(fill="both", expand=True, padx=self._p(28), pady=self._p(24))
        badge = tk.Label(body, text=icon, font=("Segoe UI Semibold", 16), bg=accent, fg="white",
                         width=2, height=1)
        badge.pack(anchor="w")
        self._label(body, title, 16, weight="bold").pack(anchor="w", pady=self._p((12, 6)))
        self._label(body, message, 10, self.MUTED, justify="left", wraplength=self._p(380)).pack(anchor="w")
        close = lambda: self._close_modal(dialog, parent if parent is not self.root else None)
        self._button(body, "OK", close).pack(anchor="e", pady=self._p((16, 0)))
        dialog.protocol("WM_DELETE_WINDOW", close)

    def _prompt_nickname(self) -> str | None:
        dialog = self._modal("Welcome to File Sharing", 460, 285)
        result = {"value": None}
        body = tk.Frame(dialog, bg=self.PANEL); body.pack(fill="both", expand=True, padx=self._p(30), pady=self._p(26))
        self._label(body, "Choose your nickname", 18, weight="bold").pack(anchor="w")
        self._label(body, "Friends will see this name next to your online status.", 10, self.MUTED,
                    wraplength=self._p(400), justify="left").pack(anchor="w", pady=self._p((6, 18)))
        entry = tk.Entry(body, bg=self.CARD, fg=self.TEXT, insertbackground=self.TEXT, relief="flat",
                         font=("Segoe UI", 12))
        entry.pack(fill="x", ipady=self._p(10)); entry.focus_set()
        guidance = self._label(body, "Enter 1–24 characters", 9, self.MUTED); guidance.pack(anchor="w", pady=self._p((6, 12)))
        def submit(_event=None):
            value = entry.get().strip()
            if not value:
                guidance.config(text="Please enter a nickname", fg=self.RED); return
            result["value"] = value[:24]
            dialog.destroy()
        self._button(body, "Continue", submit).pack(anchor="e")
        entry.bind("<Return>", submit)
        dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)
        dialog.wait_window()
        return result["value"]

    def _label(self, parent, text="", size=11, color=None, **kwargs):
        return tk.Label(parent, text=text, font=("Segoe UI", size, kwargs.pop("weight", "normal")),
                        bg=kwargs.pop("bg", parent.cget("bg")), fg=color or self.TEXT, **kwargs)

    def _button(self, parent, text, command, color=None, **kwargs):
        padx = self._p(kwargs.pop("padx", 18))
        pady = self._p(kwargs.pop("pady", 9))
        return tk.Button(parent, text=text, command=command, bg=color or self.ACCENT, fg="white", activebackground=color or self.ACCENT,
                         activeforeground="white", relief="flat", bd=0, font=("Segoe UI Semibold", 10), cursor="hand2", padx=padx, pady=pady, **kwargs)

    def _build(self):
        style = ttk.Style(self.root)
        style.theme_use("clam")
        style.configure("Upload.Horizontal.TProgressbar", troughcolor="#2a3040", background=self.ACCENT,
                        bordercolor="#2a3040", lightcolor=self.ACCENT, darkcolor=self.ACCENT, thickness=self._p(8))
        style.configure("Download.Horizontal.TProgressbar", troughcolor="#2a3040", background=self.GREEN,
                        bordercolor="#2a3040", lightcolor=self.GREEN, darkcolor=self.GREEN, thickness=self._p(8))
        sidebar = tk.Frame(self.root, bg=self.PANEL, width=self._p(280))
        sidebar.pack(side="left", fill="y"); sidebar.pack_propagate(False)
        self._label(sidebar, "FILE SHARING", 15, weight="bold").pack(anchor="w", padx=self._p(24), pady=self._p((24, 2)))
        self.status = self._label(sidebar, "● Connecting…", 9, self.MUTED)
        self.status.pack(anchor="w", padx=self._p(24))
        code_card = tk.Frame(sidebar, bg=self.CARD)
        code_card.pack(fill="x", padx=self._p(18), pady=self._p(22))
        self._label(code_card, "YOUR CODE", 8, self.MUTED).pack(pady=self._p((14, 0)))
        self.nickname_label = self._label(code_card, self.config["name"], 11, weight="bold")
        self.nickname_label.pack(pady=self._p((4, 0)))
        self.code_label = self._label(code_card, self.config["code"], 28, weight="bold")
        self.code_label.pack(pady=self._p((0, 12)))
        add = tk.Frame(sidebar, bg=self.PANEL); add.pack(fill="x", padx=self._p(18))
        self.code_entry = tk.Entry(add, bg=self.CARD, fg=self.TEXT, insertbackground=self.TEXT, relief="flat", justify="center",
                                   font=("Segoe UI Semibold", 12), width=8)
        self.code_entry.pack(side="left", fill="x", expand=True, ipady=self._p(9))
        self._button(add, "+", self._add_connection, padx=14).pack(side="left", padx=self._p((8, 0)))
        self._label(sidebar, "CONNECTIONS", 9, self.MUTED, weight="bold").pack(anchor="w", padx=self._p(24), pady=self._p((24, 8)))
        self.connection_list = tk.Frame(sidebar, bg=self.PANEL)
        self.connection_list.pack(fill="both", expand=True, padx=self._p(12))
        self._button(sidebar, "Profile & server", self._server_settings, self.CARD).pack(fill="x", padx=self._p(18), pady=self._p(18))

        main = tk.Frame(self.root, bg=self.BG); main.pack(side="left", fill="both", expand=True)
        top = tk.Frame(main, bg=self.BG); top.pack(fill="x", padx=self._p(34), pady=self._p((28, 12)))
        self.title = self._label(top, "Choose a connection", 20, weight="bold"); self.title.pack(anchor="w")
        self.subtitle = self._label(top, "Select someone on the left to send ZIP files.", 10, self.MUTED); self.subtitle.pack(anchor="w", pady=self._p((4, 0)))
        self.drop = tk.Frame(main, bg=self.PANEL, highlightbackground="#343b4a", highlightthickness=self._p(1))
        self.drop.pack(fill="both", expand=True, padx=self._p(34), pady=self._p(18))
        center = tk.Frame(self.drop, bg=self.PANEL); center.place(relx=.5, rely=.47, anchor="center")
        self._label(center, "⇧", 42, self.ACCENT).pack()
        self._label(center, "Drop ZIP files here", 17, weight="bold").pack(pady=self._p((4, 4)))
        self._label(center, "or select several files from Explorer", 10, self.MUTED).pack()
        self._button(center, "Choose ZIP files", self._choose_files).pack(pady=self._p(18))
        progress_area = tk.Frame(main, bg=self.BG)
        progress_area.pack(fill="x", padx=self._p(34), pady=self._p((0, 12)))
        self.progress_widgets = {}
        for column, (direction, heading) in enumerate((("outgoing", "OUTGOING QUEUE"), ("incoming", "INCOMING QUEUE"))):
            card = tk.Frame(progress_area, bg=self.PANEL)
            card.grid(row=0, column=column, sticky="nsew", padx=self._p((0, 7) if column == 0 else (7, 0)))
            progress_area.columnconfigure(column, weight=1)
            title = self._label(card, heading, 9, self.MUTED, weight="bold"); title.pack(anchor="w", padx=self._p(14), pady=self._p((10, 3)))
            filename = self._label(card, "No active transfer", 9, self.TEXT); filename.pack(anchor="w", padx=self._p(14))
            widgets = {"filename": filename}
            for key, label, style_name in (("uploaded", "Uploaded by sender", "Upload.Horizontal.TProgressbar"),
                                            ("downloaded", "Downloaded by receiver", "Download.Horizontal.TProgressbar")):
                line = tk.Frame(card, bg=self.PANEL); line.pack(fill="x", padx=self._p(14), pady=self._p((6, 0)))
                self._label(line, label, 8, self.MUTED).pack(side="left")
                percent = self._label(line, "0.0%", 8, self.MUTED); percent.pack(side="right")
                bar = ttk.Progressbar(card, maximum=100, value=0, style=style_name)
                bar.pack(fill="x", padx=self._p(14), pady=self._p((2, 4 if key == "downloaded" else 0)))
                widgets[key] = bar; widgets[key + "_label"] = percent
            self.progress_widgets[direction] = widgets
        self.activity = self._label(main, "Ready", 9, self.MUTED)
        self.activity.pack(anchor="w", padx=self._p(34), pady=self._p((0, 22)))
        if DND_FILES:
            self.drop.drop_target_register(DND_FILES)
            self.drop.dnd_bind("<<Drop>>", lambda event: self._send_paths(list(self.root.tk.splitlist(event.data))))

    def _render_connections(self):
        for child in self.connection_list.winfo_children(): child.destroy()
        if not self.config.get("connections"):
            self._label(self.connection_list, "No connections yet", 10, self.TEXT).pack(anchor="w", padx=self._p(12), pady=self._p((12, 3)))
            self._label(self.connection_list, "Enter a 3-digit code above\nto add someone.", 9, self.MUTED,
                        justify="left").pack(anchor="w", padx=self._p(12))
            return
        for item in self.config.get("connections", []):
            row = tk.Frame(self.connection_list, bg=self.PANEL, cursor="hand2")
            row.pack(fill="x", pady=self._p(2))
            dot = "●"; color = self.GREEN if item.get("online") else self.RED
            self._label(row, dot, 9, color).pack(side="left", padx=self._p((10, 8)), pady=self._p(10))
            self._label(row, item.get("name", "User"), 10).pack(side="left")
            state = "Online" if item.get("online") else "Offline"
            self._label(row, f"{state}  ·  {item['code']}", 9, self.MUTED).pack(side="right", padx=self._p(10))
            for widget in (row, *row.winfo_children()):
                widget.bind("<Button-1>", lambda _, code=item["code"], name=item.get("name", "User"): self._select(code, name))

    def _select(self, code, name):
        self.selected_code = code
        self.title.config(text=name)
        self.subtitle.config(text=f"Secure connection • code {code}")

    def _add_connection(self):
        code = self.code_entry.get().strip()
        if len(code) != 3 or not code.isdigit():
            self._alert("Check the connection code", "Enter exactly three digits, for example 042.")
            return
        self.network.command({"type": "add_connection", "code": code})
        self.code_entry.delete(0, "end")

    def _server_settings(self):
        dialog = self._modal("Profile & server", 420, 390)
        self._label(dialog, "Profile & relay server", 17, weight="bold").pack(anchor="w", padx=self._p(28), pady=self._p((24, 12)))
        form = tk.Frame(dialog, bg=self.PANEL); form.pack(fill="x", padx=self._p(28))
        self._label(form, "Nickname", 9, self.MUTED).grid(row=0, column=0, columnspan=2, sticky="w")
        nickname = tk.Entry(form, bg=self.CARD, fg=self.TEXT, insertbackground=self.TEXT, relief="flat", font=("Segoe UI", 10))
        nickname.grid(row=1, column=0, columnspan=2, sticky="ew", ipady=self._p(8)); nickname.insert(0, self.config["name"])
        self._label(form, "Address", 9, self.MUTED).grid(row=2, column=0, sticky="w", pady=self._p((12, 0)))
        self._label(form, "Port", 9, self.MUTED).grid(row=2, column=1, sticky="w", padx=self._p((12, 0)), pady=self._p((12, 0)))
        host = tk.Entry(form, bg=self.CARD, fg=self.TEXT, insertbackground=self.TEXT, relief="flat", font=("Segoe UI", 10))
        port = tk.Entry(form, bg=self.CARD, fg=self.TEXT, insertbackground=self.TEXT, relief="flat", width=8, font=("Segoe UI", 10))
        host.grid(row=3, column=0, sticky="ew", ipady=self._p(8)); port.grid(row=3, column=1, padx=self._p((12, 0)), ipady=self._p(8))
        form.columnconfigure(0, weight=1)
        default_selected = {"value": bool(self.config.get("use_default_server", False))}

        def show_default():
            default_selected["value"] = True
            host.configure(show="")
            port.configure(show="")
            host.delete(0, "end"); host.insert(0, "••••••••••••")
            port.delete(0, "end"); port.insert(0, "•••••")

        def begin_custom(event):
            if not default_selected["value"]:
                return
            if event.keysym in {"Tab", "ISO_Left_Tab", "Shift_L", "Shift_R", "Control_L", "Control_R",
                                "Alt_L", "Alt_R", "Left", "Right", "Home", "End", "Escape", "Return"}:
                return
            default_selected["value"] = False
            host.delete(0, "end")
            port.delete(0, "end")

        host.bind("<KeyPress>", begin_custom)
        port.bind("<KeyPress>", begin_custom)
        if default_selected["value"]:
            show_default()
        else:
            host.insert(0, self.config["server"])
            port.insert(0, str(self.config["port"]))
        hint_row = tk.Frame(dialog, bg=self.PANEL); hint_row.pack(fill="x", padx=self._p(28), pady=self._p(12))
        self._label(hint_row, "Built-in relay", 9, self.MUTED).pack(side="left")
        self._button(hint_row, "Use default", show_default, self.CARD, padx=10, pady=5).pack(side="right")
        self._label(dialog, "Changing the address resets the saved certificate pin.", 9, self.MUTED).pack(anchor="w", padx=self._p(28))
        def apply():
            if default_selected["value"]:
                new_host, new_port = DEFAULT_SERVER, DEFAULT_PORT
            else:
                try: new_port = int(port.get())
                except ValueError:
                    self._alert("Check the server port", "The port must be a number between 1 and 65535.", parent=dialog); return
                new_host = host.get().strip()
            new_name = nickname.get().strip()
            if not new_host or not 1 <= new_port <= 65535:
                self._alert("Check the server details", "Enter a valid address and a port between 1 and 65535.", parent=dialog); return
            if not new_name:
                self._alert("Choose a nickname", "Your nickname cannot be empty.", parent=dialog); return
            if (new_host, new_port) != (self.config["server"], self.config["port"]):
                self.config["server_fingerprint"] = None
            self.config.update(server=new_host, port=new_port, use_default_server=default_selected["value"],
                               name=new_name[:24]); save_config(self.config)
            self.nickname_label.config(text=self.config["name"])
            self.network.command({"type": "reconnect"}); dialog.destroy()
        self._button(dialog, "Save and reconnect", apply).pack(anchor="e", padx=self._p(28), pady=self._p((14, 10)))

    def _choose_files(self):
        paths = filedialog.askopenfilenames(title="Choose ZIP files", filetypes=[("ZIP archives", "*.zip")])
        self._send_paths(paths)

    def _send_paths(self, paths):
        if not self.selected_code:
            self._alert("Choose a connection", "Select the person who should receive these files first.", tone="info")
            return
        valid = [p for p in paths if Path(p).is_file() and Path(p).suffix.lower() == ".zip" and zipfile.is_zipfile(p)]
        invalid = [p for p in paths if p not in valid]
        if invalid:
            self._alert("Some files were skipped", "Only valid ZIP archives can be sent.", tone="info")
        if valid:
            self.network.command({"type": "queue_files", "recipient": self.selected_code, "paths": valid})
            if self.is_online:
                self.activity.config(text=f"Queued {len(valid)} file(s) for sequential sending")
            else:
                self.activity.config(text=f"Queued {len(valid)} file(s) — sending will begin when the server reconnects")

    def _set_progress(self, event):
        widgets = self.progress_widgets[event.get("direction", "outgoing")]
        size = max(1, int(event.get("size", 0)))
        widgets["filename"].config(text=event.get("filename", "Transfer"))
        for key in ("uploaded", "downloaded"):
            value = min(100.0, 100.0 * int(event.get(key, 0)) / size)
            widgets[key].configure(value=value)
            widgets[key + "_label"].config(text=f"{value:.1f}%")

    def _incoming(self, item, ready=False):
        dialog = self._modal("Incoming file", 430, 235)
        self._label(dialog, "Incoming ZIP file", 17, weight="bold").pack(pady=self._p((24, 6)))
        self._label(dialog, f"{item.get('sender_name', 'User')} wants to send", 10, self.MUTED).pack()
        self._label(dialog, item["filename"], 12, weight="bold").pack(pady=self._p((8, 2)))
        self._label(dialog, f"{item['size'] / 1048576:.1f} MB", 9, self.MUTED).pack()
        buttons = tk.Frame(dialog, bg=self.PANEL); buttons.pack(pady=self._p(20))
        def receive():
            folder = filedialog.askdirectory(parent=dialog, title="Choose download folder")
            if folder:
                if ready:
                    destination = available_path(folder, item["filename"])
                    partial = destination.with_suffix(destination.suffix + ".part")
                    stream = partial.open("wb")
                    self.network.downloads[item["transfer_id"]] = {
                        "destination": destination, "stream": stream, "digest": hashlib.sha256(),
                        "size": int(item["size"]), "received": 0, "filename": item["filename"]}
                    self.network.command({"type": "download", "transfer_id": item["transfer_id"]})
                else:
                    self.network.command({"type": "accept", "transfer_id": item["transfer_id"], "folder": folder,
                                          "filename": item["filename"], "size": item["size"]})
                dialog.destroy()
        self._button(buttons, "Reject", lambda: (self.network.command({"type": "reject", "transfer_id": item["transfer_id"]}), dialog.destroy()), self.RED).pack(side="left", padx=self._p(6))
        self._button(buttons, "Receive", receive, self.GREEN).pack(side="left", padx=self._p(6))

    def _poll(self):
        try:
            while True:
                event = self.events.get_nowait(); kind = event.get("type")
                if kind == "network":
                    online = event["online"]
                    self.is_online = online
                    self.status.config(text="● Connected" if online else "● Server unavailable", fg=self.GREEN if online else self.RED)
                    if online:
                        if not self.network.file_queue:
                            self.activity.config(text="Ready to send files")
                    else:
                        self.activity.config(text="Check your connection or server settings — retrying automatically")
                elif kind == "welcome":
                    self.code_label.config(text=event["code"])
                elif kind in {"connections", "connection_added", "presence"}:
                    self._render_connections()
                elif kind == "incoming_offer":
                    self._set_progress({**event, "direction": "incoming", "uploaded": 0, "downloaded": 0})
                    self._incoming(event)
                elif kind == "transfer_ready":
                    self._set_progress({**event, "direction": "incoming", "uploaded": event["size"], "downloaded": 0})
                    self._incoming(event, ready=True)
                elif kind == "transfer_progress": self._set_progress(event)
                elif kind == "sending_status": self.activity.config(text=event["text"])
                elif kind == "upload_complete": self.activity.config(text="Upload secured in relay buffer")
                elif kind == "delivered": self.activity.config(text="Delivered successfully")
                elif kind == "received_file": self._alert("Download complete", f"Saved to:\n{event['path']}", tone="info")
                elif kind == "rejected": self.activity.config(text=f"{event['filename']} was rejected")
                elif kind == "error": self._alert("Unable to complete that action", friendly_error(event.get("message", "")))
        except queue.Empty:
            pass
        self.code_label.config(text=self.config.get("code", "---"))
        self.root.after(80, self._poll)

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    App().run()
