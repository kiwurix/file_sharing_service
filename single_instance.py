"""Single-instance guard for the Windows desktop client."""

from __future__ import annotations

import ctypes
import os

MUTEX_NAME = "Local\\FileSharingServiceClient"
ERROR_ALREADY_EXISTS = 183


class SingleInstanceGuard:
    def __init__(self, name: str = MUTEX_NAME):
        self.handle = None
        self.is_primary = True
        if os.name != "nt":
            return
        kernel32 = ctypes.windll.kernel32
        kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p]
        kernel32.CreateMutexW.restype = ctypes.c_void_p
        self.handle = kernel32.CreateMutexW(None, False, name)
        self.is_primary = bool(self.handle) and kernel32.GetLastError() != ERROR_ALREADY_EXISTS
        if not self.is_primary and self.handle:
            kernel32.CloseHandle(self.handle)
            self.handle = None

    def close(self) -> None:
        if self.handle and os.name == "nt":
            ctypes.windll.kernel32.CloseHandle(self.handle)
            self.handle = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def focus_existing_window(title: str = "File Sharing") -> None:
    if os.name != "nt":
        return
    user32 = ctypes.windll.user32
    user32.FindWindowW.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p]
    user32.FindWindowW.restype = ctypes.c_void_p
    window = user32.FindWindowW(None, title)
    if window:
        user32.ShowWindow(window, 9)  # SW_RESTORE
        user32.SetForegroundWindow(window)
