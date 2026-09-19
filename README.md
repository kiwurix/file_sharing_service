# File Sharing Service

Encrypted ZIP-file sharing with a lightweight desktop client and a persistent relay server.

## Features

- Stable three-digit user codes
- Nicknames with green/red online indicators
- Drag-and-drop or Explorer file selection
- Sequential multi-file sending
- Simultaneous transfers in both directions
- TLS-encrypted traffic with certificate pinning
- SHA-256 integrity verification
- Disk-backed relay buffer limited to 1 GiB per user pair
- Maximum five connections per user
- Exact upload/download progress with network updates throttled to once per second
- DPI-aware, resolution-adaptive interface from laptop displays through 4K
- ZIP files only

## Project layout

| Path | Purpose |
| --- | --- |
| `server.py` | TLS relay, identity registry, connections, and buffered queues |
| `client.py` | Desktop interface and network client |
| `protocol.py` | Shared framed network protocol |
| `main.py` | Client entry point |
| `tests/` | Unit and end-to-end TLS transfer tests |
| `buffer/` | Runtime-only server state, certificate, key, and pending files |

## Logs

- Server: `buffer/logs/server.log`
- Client: `%LOCALAPPDATA%\file_sharing\logs\client.log`

Logs rotate at 5 MiB and retain three backups. They include startup, connection state, rejected requests, transfer milestones, integrity checks, delivery, and cleanup. Identity tokens, certificate fingerprints, and file contents are never logged.

## Server deployment

Requirements:

- Python 3.11 or newer
- One writable directory for this project
- Inbound TCP port `8765`, or another configured port

On Linux:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python server.py --host 0.0.0.0 --port 8765 --buffer ./buffer
```

On Windows Server:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe server.py --host 0.0.0.0 --port 8765 --buffer .\buffer
```

The first start creates the following inside `buffer/`:

- `state.json`: user identities, codes, nicknames, connections, and transfer metadata
- `server-cert.pem`: self-signed TLS certificate
- `server-key.pem`: private TLS key
- `files/`: incomplete and pending transfers

Do not delete or replace `state.json`, `server-cert.pem`, or `server-key.pem` during normal updates. Deleting `state.json` resets every user code and connection. Replacing the certificate causes existing clients to reject the server until their saved certificate pin is reset.

### systemd service

Adjust the user and paths before installing:

```ini
[Unit]
Description=File Sharing Relay
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=file-sharing
WorkingDirectory=/opt/file-sharing
ExecStart=/opt/file-sharing/.venv/bin/python /opt/file-sharing/server.py --host 0.0.0.0 --port 8765 --buffer /opt/file-sharing/buffer
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

Save it as `/etc/systemd/system/file-sharing.service`, then run:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now file-sharing
sudo systemctl status file-sharing
```

View logs with:

```bash
journalctl -u file-sharing -f
```

## Client setup

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe main.py
```

On first launch, enter a nickname. The client defaults to `localhost:52007`. Open **Profile & server** to enter a custom relay or use **Use default** to restore the built-in address, then save and reconnect.

Client state is stored at:

```text
%LOCALAPPDATA%\file_sharing\connections.json
```

This file contains the stable client token, assigned code, nickname, server certificate fingerprint, and cached connections. Back it up if the client identity must survive an OS reinstall.

Changing IP addresses does not affect identities or connections. They remain stable while both the client `connections.json` and server `buffer/state.json` are preserved.

## Validation

Run the complete test suite:

```bash
python -m unittest discover -v
```

The suite verifies identity persistence, code stability, TLS certificate generation, ZIP validation, framed transport, progress throttling, and the complete offer/upload/download/cleanup lifecycle.

## Backup and updates

1. Stop the relay.
2. Back up the entire `buffer/` directory.
3. Update the Python source files and dependencies.
4. Keep the existing `buffer/` directory unchanged.
5. Run the tests.
6. Restart the relay and inspect its logs.

Queued files may be active while the service is running, so do not copy or modify `buffer/` during a live backup.

## Operating rules for automation agents

- Treat `buffer/` as persistent production data.
- Never regenerate identity state or TLS keys unless explicitly requested.
- Never expose `server-key.pem` or client tokens in logs.
- Do not manually edit `state.json` while the server is running.
- Use a temporary directory for tests; tests must not point at the production buffer.
- Preserve `connections.json` during client updates.
- Confirm the configured port is reachable after deployment.
- Verify the test suite before restarting production after an update.
