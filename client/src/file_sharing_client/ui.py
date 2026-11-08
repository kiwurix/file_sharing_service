"""Tk desktop interface. Network operations run on a separate event loop."""

from __future__ import annotations

import queue
import time
import zipfile
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk

from file_sharing_common.logging import configure_logging
from .config import ClientSettings, StateStore
from .display import calculate_display_metrics, enable_high_dpi
from .i18n import STRINGS, friendly_error, translate
from .network import NetworkClient

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
except ImportError:
    DND_FILES = None
    TkinterDnD = None

class App:
    BG, PANEL, CARD, TEXT, MUTED, ACCENT, GREEN, RED = "#10131a", "#171b24", "#202632", "#f4f6fb", "#929bad", "#6c7cff", "#42d392", "#ff647c"

    def __init__(self, settings: ClientSettings):
        self.settings = settings
        self.store = StateStore(settings)
        enable_high_dpi()
        root_class = TkinterDnD.Tk if TkinterDnD else tk.Tk
        self.root = root_class()
        self.config = self.store.load()
        self.language = self.config.get("language", "en")
        self.root.title(self._t("app_title"))
        dpi_scale = self.root.winfo_fpixels("1i") / 96.0
        metrics = calculate_display_metrics(self.root.winfo_screenwidth(), self.root.winfo_screenheight(), dpi_scale)
        self.ui_scale = metrics["dpi_scale"]
        x = max(0, (self.root.winfo_screenwidth() - metrics["width"]) // 2)
        y = max(0, (self.root.winfo_screenheight() - metrics["height"]) // 2)
        self.root.geometry(f"{metrics['width']}x{metrics['height']}+{x}+{y}")
        self.root.minsize(min(self._p(780), metrics["width"]), min(self._p(520), metrics["height"]))
        self.root.configure(bg=self.BG)
        self.log = configure_logging("file_sharing.client", self.settings.state_dir / "logs" / "client.log")
        self.log.info("Client starting: server=%s port=%s", self.config["server"], self.config["port"])
        if not str(self.config.get("name", "")).strip():
            self.root.withdraw()
            nickname = self._prompt_nickname()
            if not nickname or not nickname.strip():
                self.root.destroy()
                raise SystemExit("A nickname is required")
            self.config["name"] = nickname.strip()[:24]
            self.store.save(self.config)
            self.root.deiconify()
        self.events: queue.Queue = queue.Queue()
        self.network = NetworkClient(self.config, self.events, self.log, self.settings.state_dir / "logs",
                                     save_state=self.store.save)
        self.connection_rows: dict[str, tk.Frame] = {}
        self.selected_code: str | None = None
        self.selected_name: str | None = None
        self.progress_samples: dict[str, dict] = {}
        self.is_online = False
        self.last_latency_ms: int | None = None
        self._build()
        self._render_connections()
        self.network.start()
        self.root.after(80, self._poll)

    def _p(self, value):
        """Scale logical pixels to the monitor's physical DPI."""
        if isinstance(value, tuple):
            return tuple(self._p(part) for part in value)
        return round(value * self.ui_scale)

    def _t(self, key: str, **values) -> str:
        return translate(self.language, key, **values)

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
        self._button(body, self._t("ok"), close).pack(anchor="e", pady=self._p((16, 0)))
        dialog.protocol("WM_DELETE_WINDOW", close)

    def _prompt_nickname(self) -> str | None:
        dialog = self._modal(self._t("welcome_title"), 460, 285)
        result = {"value": None}
        body = tk.Frame(dialog, bg=self.PANEL); body.pack(fill="both", expand=True, padx=self._p(30), pady=self._p(26))
        self._label(body, self._t("choose_nickname"), 18, weight="bold").pack(anchor="w")
        self._label(body, self._t("nickname_help"), 10, self.MUTED,
                    wraplength=self._p(400), justify="left").pack(anchor="w", pady=self._p((6, 18)))
        entry = tk.Entry(body, bg=self.CARD, fg=self.TEXT, insertbackground=self.TEXT, relief="flat",
                         font=("Segoe UI", 12))
        entry.pack(fill="x", ipady=self._p(10)); entry.focus_set()
        guidance = self._label(body, self._t("nickname_guidance"), 9, self.MUTED); guidance.pack(anchor="w", pady=self._p((6, 12)))
        def submit(_event=None):
            value = entry.get().strip()
            if not value:
                guidance.config(text=self._t("nickname_required"), fg=self.RED); return
            result["value"] = value[:24]
            dialog.destroy()
        self._button(body, self._t("continue"), submit).pack(anchor="e")
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

    def _rebuild_interface(self) -> None:
        for child in self.root.winfo_children():
            child.destroy()
        self.root.title(self._t("app_title"))
        self._build()
        self._render_connections()
        if self.selected_code and self.selected_name:
            self.title.config(text=self.selected_name)
            self.subtitle.config(text=self._t("secure_connection", code=self.selected_code))
        if self.is_online:
            if self.last_latency_ms is None:
                self.status.config(text=self._t("connected"), fg=self.GREEN)
            else:
                self.status.config(text=self._t("connected_latency", latency=self.last_latency_ms), fg=self.GREEN)
            if not self.network.file_queue:
                self.activity.config(text=self._t("ready_to_send"))
        else:
            self.status.config(text=self._t("server_unavailable"), fg=self.RED)

    def _build(self):
        style = ttk.Style(self.root)
        style.theme_use("clam")
        style.configure("Upload.Horizontal.TProgressbar", troughcolor="#2a3040", background=self.ACCENT,
                        bordercolor="#2a3040", lightcolor=self.ACCENT, darkcolor=self.ACCENT, thickness=self._p(8))
        style.configure("Download.Horizontal.TProgressbar", troughcolor="#2a3040", background=self.GREEN,
                        bordercolor="#2a3040", lightcolor=self.GREEN, darkcolor=self.GREEN, thickness=self._p(8))
        sidebar = tk.Frame(self.root, bg=self.PANEL, width=self._p(280))
        sidebar.pack(side="left", fill="y"); sidebar.pack_propagate(False)
        self._label(sidebar, self._t("brand"), 15, weight="bold").pack(anchor="w", padx=self._p(24), pady=self._p((24, 2)))
        self.status = self._label(sidebar, self._t("connecting"), 9, self.MUTED)
        self.status.pack(anchor="w", padx=self._p(24))
        code_card = tk.Frame(sidebar, bg=self.CARD)
        code_card.pack(fill="x", padx=self._p(18), pady=self._p(22))
        self._label(code_card, self._t("your_code"), 8, self.MUTED).pack(pady=self._p((14, 0)))
        self.nickname_label = self._label(code_card, self.config["name"], 11, weight="bold")
        self.nickname_label.pack(pady=self._p((4, 0)))
        self.code_label = self._label(code_card, self.config["code"], 28, weight="bold")
        self.code_label.pack(pady=self._p((0, 12)))
        add = tk.Frame(sidebar, bg=self.PANEL); add.pack(fill="x", padx=self._p(18))
        self.code_entry = tk.Entry(add, bg=self.CARD, fg=self.TEXT, insertbackground=self.TEXT, relief="flat", justify="center",
                                   font=("Segoe UI Semibold", 12), width=8)
        self.code_entry.pack(side="left", fill="x", expand=True, ipady=self._p(9))
        self._button(add, "+", self._add_connection, padx=14).pack(side="left", padx=self._p((8, 0)))
        self._label(sidebar, self._t("connections"), 9, self.MUTED, weight="bold").pack(anchor="w", padx=self._p(24), pady=self._p((24, 8)))
        self.connection_list = tk.Frame(sidebar, bg=self.PANEL)
        self.connection_list.pack(fill="both", expand=True, padx=self._p(12))
        self._button(sidebar, self._t("profile_server"), self._server_settings, self.CARD).pack(fill="x", padx=self._p(18), pady=self._p(18))

        main_shell = tk.Frame(self.root, bg=self.BG)
        main_shell.pack(side="left", fill="both", expand=True)
        canvas = tk.Canvas(main_shell, bg=self.BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(main_shell, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        main = tk.Frame(canvas, bg=self.BG)
        main_window = canvas.create_window((0, 0), window=main, anchor="nw")
        main.bind("<Configure>", lambda _event: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>", lambda event: canvas.itemconfigure(main_window, width=event.width))
        canvas.bind_all("<MouseWheel>", lambda event: canvas.yview_scroll(-int(event.delta / 120), "units"))
        top = tk.Frame(main, bg=self.BG); top.pack(fill="x", padx=self._p(34), pady=self._p((28, 12)))
        self.title = self._label(top, self._t("choose_connection"), 20, weight="bold"); self.title.pack(anchor="w")
        self.subtitle = self._label(top, self._t("select_left"), 10, self.MUTED); self.subtitle.pack(anchor="w", pady=self._p((4, 0)))
        self.drop = tk.Frame(main, bg=self.PANEL, highlightbackground="#343b4a", highlightthickness=self._p(1))
        self.drop.pack(fill="x", padx=self._p(34), pady=self._p(18))
        self.drop.configure(height=self._p(250))
        self.drop.pack_propagate(False)
        center = tk.Frame(self.drop, bg=self.PANEL); center.place(relx=.5, rely=.47, anchor="center")
        self._label(center, "⇧", 42, self.ACCENT).pack()
        self._label(center, self._t("drop_zip"), 17, weight="bold").pack(pady=self._p((4, 4)))
        self._label(center, self._t("or_explorer"), 10, self.MUTED).pack()
        self._button(center, self._t("choose_zip_files"), self._choose_files).pack(pady=self._p(18))
        progress_area = tk.Frame(main, bg=self.BG)
        progress_area.pack(fill="x", padx=self._p(34), pady=self._p((0, 12)))
        self.progress_widgets = {}
        for row, (direction, heading) in enumerate((("outgoing", self._t("outgoing_queue")),
                                                    ("incoming", self._t("incoming_queue")))):
            card = tk.Frame(progress_area, bg=self.PANEL)
            card.grid(row=row, column=0, sticky="ew", pady=self._p((0, 7) if row == 0 else (7, 0)))
            progress_area.columnconfigure(0, weight=1)
            title = self._label(card, heading, 9, self.MUTED, weight="bold"); title.pack(anchor="w", padx=self._p(14), pady=self._p((10, 3)))
            filename = self._label(card, self._t("no_active_transfer"), 9, self.TEXT); filename.pack(anchor="w", padx=self._p(14))
            widgets = {"filename": filename}
            line = tk.Frame(card, bg=self.PANEL); line.pack(fill="x", padx=self._p(14), pady=self._p((6, 0)))
            self._label(line, self._t("transfer_progress"), 8, self.MUTED).pack(side="left")
            progress_label = self._label(line, "0.0% · 0.0 Mb/s", 8, self.MUTED); progress_label.pack(side="right")
            bar = ttk.Progressbar(card, maximum=100, value=0,
                                  style="Upload.Horizontal.TProgressbar" if direction == "outgoing"
                                  else "Download.Horizontal.TProgressbar")
            bar.pack(fill="x", padx=self._p(14), pady=self._p((2, 8)))
            widgets.update(bar=bar, progress_label=progress_label)
            self.progress_widgets[direction] = widgets
        self.activity = self._label(main, self._t("ready"), 9, self.MUTED)
        self.activity.pack(anchor="w", padx=self._p(34), pady=self._p((0, 22)))
        if DND_FILES:
            self.drop.drop_target_register(DND_FILES)
            self.drop.dnd_bind("<<Drop>>", lambda event: self._send_paths(list(self.root.tk.splitlist(event.data))))

    def _render_connections(self):
        for child in self.connection_list.winfo_children(): child.destroy()
        if not self.config.get("connections"):
            self._label(self.connection_list, self._t("no_connections"), 10, self.TEXT).pack(anchor="w", padx=self._p(12), pady=self._p((12, 3)))
            self._label(self.connection_list, self._t("enter_code"), 9, self.MUTED,
                        justify="left").pack(anchor="w", padx=self._p(12))
            return
        for item in self.config.get("connections", []):
            row = tk.Frame(self.connection_list, bg=self.PANEL, cursor="hand2")
            row.pack(fill="x", pady=self._p(2))
            known = self.is_online
            dot = "●"; color = self.GREEN if known and item.get("online") else self.RED if known else self.MUTED
            self._label(row, dot, 9, color).pack(side="left", padx=self._p((10, 8)), pady=self._p(10))
            self._label(row, item.get("name", self._t("user")), 10).pack(side="left")
            state = self._t("online") if known and item.get("online") else self._t("offline") if known else self._t("status_unknown")
            self._label(row, f"{state}  ·  {item['code']}", 9, self.MUTED).pack(side="right", padx=self._p(10))
            for widget in (row, *row.winfo_children()):
                widget.bind("<Button-1>", lambda _, code=item["code"], name=item.get("name", self._t("user")): self._select(code, name))

    def _select(self, code, name):
        self.selected_code = code
        self.selected_name = name
        self.title.config(text=name)
        self.subtitle.config(text=self._t("secure_connection", code=code))

    def _add_connection(self):
        code = self.code_entry.get().strip()
        if len(code) != 3 or not code.isdigit():
            self._alert(self._t("check_code"), self._t("check_code_body"))
            return
        self.network.command({"type": "add_connection", "code": code})
        self.code_entry.delete(0, "end")

    def _server_settings(self):
        dialog = self._modal(self._t("profile_server"), 420, 485)
        self._label(dialog, self._t("profile_title"), 17, weight="bold").pack(anchor="w", padx=self._p(28), pady=self._p((24, 12)))
        form = tk.Frame(dialog, bg=self.PANEL); form.pack(fill="x", padx=self._p(28))
        self._label(form, self._t("nickname"), 9, self.MUTED).grid(row=0, column=0, columnspan=2, sticky="w")
        nickname = tk.Entry(form, bg=self.CARD, fg=self.TEXT, insertbackground=self.TEXT, relief="flat", font=("Segoe UI", 10))
        nickname.grid(row=1, column=0, columnspan=2, sticky="ew", ipady=self._p(8)); nickname.insert(0, self.config["name"])
        self._label(form, self._t("address"), 9, self.MUTED).grid(row=2, column=0, sticky="w", pady=self._p((12, 0)))
        self._label(form, self._t("port"), 9, self.MUTED).grid(row=2, column=1, sticky="w", padx=self._p((12, 0)), pady=self._p((12, 0)))
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
        self._label(hint_row, self._t("built_in_relay"), 9, self.MUTED).pack(side="left")
        self._button(hint_row, self._t("use_default"), show_default, self.CARD, padx=10, pady=5).pack(side="right")
        self._label(dialog, self._t("certificate_hint"), 9, self.MUTED, justify="left",
                    wraplength=self._p(360)).pack(anchor="w", padx=self._p(28))
        self._label(dialog, self._t("language"), 9, self.MUTED, weight="bold").pack(anchor="w", padx=self._p(28), pady=self._p((16, 4)))
        language_var = tk.StringVar(value=self.language)
        language_row = tk.Frame(dialog, bg=self.PANEL); language_row.pack(anchor="w", padx=self._p(24))
        for label, value in (("English", "en"), ("Русский", "ru")):
            tk.Radiobutton(language_row, text=label, variable=language_var, value=value, bg=self.PANEL,
                           fg=self.TEXT, activebackground=self.PANEL, activeforeground=self.TEXT,
                           selectcolor=self.CARD, font=("Segoe UI", 10), cursor="hand2").pack(side="left", padx=self._p(4))
        def apply():
            if default_selected["value"]:
                new_host, new_port = self.settings.server, self.settings.port
            else:
                try: new_port = int(port.get())
                except ValueError:
                    self._alert(self._t("check_port"), self._t("check_port_body"), parent=dialog); return
                new_host = host.get().strip()
            new_name = nickname.get().strip()
            if not new_host or not 1 <= new_port <= 65535:
                self._alert(self._t("check_server"), self._t("check_server_body"), parent=dialog); return
            if not new_name:
                self._alert(self._t("empty_nickname"), self._t("empty_nickname_body"), parent=dialog); return
            new_language = language_var.get() if language_var.get() in STRINGS else "en"
            address_changed = (new_host, new_port) != (self.config["server"], self.config["port"])
            reconnect_required = address_changed or new_name[:24] != self.config["name"]
            language_changed = new_language != self.language
            if address_changed:
                self.config["server_fingerprint"] = None
            self.config.update(server=new_host, port=new_port, use_default_server=default_selected["value"],
                               name=new_name[:24], language=new_language)
            self.store.save(self.config)
            dialog.destroy()
            self.language = new_language
            if language_changed:
                self._rebuild_interface()
            else:
                self.nickname_label.config(text=self.config["name"])
            if reconnect_required:
                self.network.command({"type": "reconnect"})
        self._button(dialog, self._t("save_changes"), apply).pack(anchor="e", padx=self._p(28), pady=self._p((14, 10)))

    def _choose_files(self):
        paths = filedialog.askopenfilenames(title=self._t("choose_zip_files"),
                                            filetypes=[(self._t("zip_archives"), "*.zip")])
        self._send_paths(paths)

    def _send_paths(self, paths):
        if not self.selected_code:
            self._alert(self._t("choose_connection"), self._t("select_recipient"), tone="info")
            return
        valid = [p for p in paths if Path(p).is_file() and Path(p).suffix.lower() == ".zip" and zipfile.is_zipfile(p)]
        invalid = [p for p in paths if p not in valid]
        if invalid:
            self._alert(self._t("files_skipped"), self._t("valid_zip_only"), tone="info")
        if valid:
            self.network.command({"type": "queue_files", "recipient": self.selected_code, "paths": valid})
            if self.is_online:
                self.activity.config(text=self._t("queued_files", count=len(valid)))
            else:
                self.activity.config(text=self._t("queued_files_offline", count=len(valid)))

    def _set_progress(self, event):
        direction = event.get("direction", "outgoing")
        widgets = self.progress_widgets[direction]
        size = max(1, int(event.get("size", 0)))
        filename = event.get("filename", self._t("no_active_transfer"))
        transferred = max(int(event.get("uploaded", 0)), int(event.get("downloaded", 0)))
        now = time.monotonic()
        sample = self.progress_samples.get(direction)
        if not sample or sample["filename"] != filename or transferred < sample["bytes"]:
            sample = {"filename": filename, "bytes": transferred, "time": now, "speed": 0.0}
        elif transferred > sample["bytes"] and now > sample["time"]:
            instant = (transferred - sample["bytes"]) / (now - sample["time"])
            sample["speed"] = instant if not sample["speed"] else sample["speed"] * 0.7 + instant * 0.3
            sample["bytes"], sample["time"] = transferred, now
        self.progress_samples[direction] = sample
        value = min(100.0, 100.0 * transferred / size)
        widgets["filename"].config(text=filename)
        widgets["bar"].configure(value=value)
        speed_mbps = sample["speed"] * 8 / 1_000_000
        widgets["progress_label"].config(text=f"{value:.1f}% · {speed_mbps:.1f} Mb/s")

    def _incoming(self, item):
        dialog = self._modal(self._t("incoming_file"), 430, 235)
        self._label(dialog, self._t("incoming_zip"), 17, weight="bold").pack(pady=self._p((24, 6)))
        self._label(dialog, self._t("wants_to_send", sender=item.get("sender_name", self._t("user"))), 10, self.MUTED).pack()
        self._label(dialog, item["filename"], 12, weight="bold").pack(pady=self._p((8, 2)))
        self._label(dialog, self._t("size_mb", size=item["size"] / 1048576), 9, self.MUTED).pack()
        buttons = tk.Frame(dialog, bg=self.PANEL); buttons.pack(pady=self._p(20))
        def receive():
            folder = filedialog.askdirectory(parent=dialog, title=self._t("choose_download_folder"))
            if folder:
                self.network.command({"type": "accept", "transfer_id": item["transfer_id"], "folder": folder,
                                      "filename": item["filename"], "size": item["size"]})
                dialog.destroy()
        self._button(buttons, self._t("reject"), lambda: (self.network.command({"type": "reject", "transfer_id": item["transfer_id"]}), dialog.destroy()), self.RED).pack(side="left", padx=self._p(6))
        self._button(buttons, self._t("receive"), receive, self.GREEN).pack(side="left", padx=self._p(6))

    def _poll(self):
        try:
            while True:
                event = self.events.get_nowait(); kind = event.get("type")
                if kind == "network":
                    online = event["online"]
                    self.is_online = online
                    if online and event.get("verified", True):
                        self.status.config(text=self._t("connected_checking"), fg=self.GREEN)
                    elif online:
                        self.status.config(text=self._t("connected"), fg=self.GREEN)
                    else:
                        self.last_latency_ms = None
                        self.status.config(text=self._t("server_unavailable"), fg=self.RED)
                    if online:
                        if not self.network.file_queue:
                            self.activity.config(text=self._t("ready_to_send"))
                    else:
                        retry = event.get("retry_in", 3)
                        if event.get("duplicate"):
                            self.activity.config(text=self._t("identity_elsewhere"))
                        else:
                            self.activity.config(text=self._t("retrying", seconds=retry))
                    self._render_connections()
                elif kind == "health":
                    self.last_latency_ms = event["latency_ms"]
                    self.status.config(text=self._t("connected_latency", latency=self.last_latency_ms), fg=self.GREEN)
                elif kind == "welcome":
                    self.code_label.config(text=event["code"])
                elif kind in {"connections", "connection_added", "presence"}:
                    self._render_connections()
                elif kind == "incoming_offer":
                    self._set_progress({**event, "direction": "incoming", "uploaded": 0, "downloaded": 0})
                    self._incoming(event)
                elif kind == "transfer_progress": self._set_progress(event)
                elif kind == "sending_status":
                    self.activity.config(text=self._t(event.get("key", "error_generic"), **event.get("values", {})))
                elif kind == "upload_complete": self.activity.config(text=self._t("file_streamed"))
                elif kind == "delivered": self.activity.config(text=self._t("delivered"))
                elif kind == "received_file": self._alert(self._t("download_complete"), self._t("saved_to", path=event["path"]), tone="info")
                elif kind == "rejected": self.activity.config(text=self._t("file_rejected", filename=event["filename"]))
                elif kind == "error": self._alert(self._t("unable_action"), friendly_error(event.get("message", ""), self.language))
        except queue.Empty:
            pass
        self.code_label.config(text=self.config.get("code", "---"))
        self.root.after(80, self._poll)

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    App().run()
