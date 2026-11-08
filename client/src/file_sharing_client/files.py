"""Destination filename selection for incoming files."""

from pathlib import Path

def available_path(folder: str | Path, filename: str) -> Path:
    destination = Path(folder) / Path(filename).name
    counter = 1
    while destination.exists() or destination.with_suffix(destination.suffix + ".part").exists():
        destination = Path(folder) / f"{Path(filename).stem} ({counter}){Path(filename).suffix}"
        counter += 1
    return destination
