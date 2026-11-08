"""Desktop client entry point."""

import argparse
from pathlib import Path

from file_sharing_client.config import ClientSettings
from file_sharing_client.single_instance import SingleInstanceGuard, focus_existing_window


def main() -> None:
    parser = argparse.ArgumentParser(description='File Sharing desktop client')
    parser.add_argument('--config', type=Path, help='Local TOML configuration file')
    args = parser.parse_args()
    try:
        settings = ClientSettings.load(args.config)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    with SingleInstanceGuard() as instance:
        if not instance.is_primary:
            focus_existing_window()
        else:
            from file_sharing_client.ui import App
            App(settings).run()


if __name__ == '__main__':
    main()
