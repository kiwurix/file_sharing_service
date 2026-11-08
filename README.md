# File Sharing Service

A Python desktop app for sending ZIP files through a TLS relay.

- Three-digit contact codes and saved connections
- Drag-and-drop file selection and transfer queues
- Streaming transfers with SHA-256 verification
- Certificate pinning, heartbeat and automatic reconnect
- English and Russian interface

## Structure

| Package | Responsibility |
| --- | --- |
| `client/` | Desktop UI, network client, local settings and identity |
| `server/` | Relay, persistent identity registry and TLS certificate |
| `shared/` | Framed protocol, rotating logs and configuration helpers |

The client and server are independently installable applications. Both depend on the same small shared package. UI, networking, translations and persistence are separate modules.

## Install

Requires Python 3.11+. Create and activate a virtual environment first.

For the client, install Tkinter through your Python installation, then run:

```bash
python -m pip install ./shared ./client
```

For the server:

```bash
python -m pip install ./shared ./server
```

## Configure and run

Copy `client/config.example.toml` to `client/config.toml` and set `server` to your relay hostname or address. Start the client:

```bash
python -m file_sharing_client --config client/config.toml
```

Copy `server/config.example.toml` to `server/config.toml` and choose the listening interface, port and persistent data directory. Allow inbound TCP traffic on the configured port. Start the relay:

```bash
python -m file_sharing_server --config server/config.toml
```

Configuration paths are explicit. Paths inside a TOML file are relative to that file, independent of the working directory. Real config files are ignored by Git. The committed examples use local addresses.

On the first client launch, choose a nickname. Use **Profile & server** to change the relay, language or nickname. **Use default** restores the relay from the local configuration.

## State and updates

The client keeps its existing identity in `%LOCALAPPDATA%/file_sharing/connections.json`. Set `state_dir` in the client config to choose another directory. Saved identities, contacts and certificate pins are preserved. A configured relay change resets the pin for the new endpoint.

The server stores `state.json`, `server-cert.pem`, `server-key.pem` and logs in `data_dir`. Keep this directory when updating. Existing deployments should point `data_dir` at their current buffer directory to preserve user codes, contacts and certificates.

Logs rotate at 5 MiB with three backups. Clients upload diagnostic log batches to the relay. File contents, identity tokens and certificate fingerprints are excluded from log messages.

`deploy/file-sharing.service` is a Linux systemd template using a dedicated `file-sharing` account, `/opt/file-sharing` installation and `/etc/file-sharing/server.toml` configuration. Set `data_dir` to a directory writable by that account before starting the service.

## Windows build

```powershell
.\build_windows.ps1
```

Output: `dist/FileSharing.exe` and its SHA-256 checksum. Run it with `--config` pointing to your private client TOML file. The **Build Windows client** workflow runs the tests and produces the executable.

## Development

```bash
python -m pip install -r requirements-dev.txt
python -m unittest discover -v
```

The tests use temporary state directories and cover persistence, framing, TLS, progress throttling and relay transfers.

## Operating rules for automation agents

- Treat server and client state directories as private persistent data.
- Preserve `state.json`, `connections.json`, certificates and keys during updates.
- Use temporary directories for tests.
- Keep real configuration, keys, tokens, state and logs out of Git.
- Deployment and service restarts require an explicit request.
