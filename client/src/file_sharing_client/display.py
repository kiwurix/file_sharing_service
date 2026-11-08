"""Windows DPI setup and display geometry."""

import ctypes
import os

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
