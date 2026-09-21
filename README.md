# FSS (File Sharing Server)

FSS Server is the backend for a chat (discord like) platform focused on file sharing and archiving built in Rust.
It provides a WebSocket command channel for most application actions and authenticated HTTP API for file uploads.

## What It Does

- user registration, login, logout, and token renewal
- server and space creation, deletion, joining, and leaving
- direct messages and space messages
- friend requests and friend management
- moderation actions such as bans and unbans
- server and space notifications over Redis-backed pub/sub
- file uploads into a space

## Requirements

- Rust toolchain with Cargo
- Postgres
- Redis
- S3-compatible storage such as MinIO

The repository includes a `docker-compose.yaml` that starts local Postgres, Redis, MinIO, and a other few helper services.

## Configuration

The binary expects a JSON config file passed with `-c` or `--config`.

Example:

```bash
cargo run -- -c examples/config.json

```

Example config fields:

- `minio.url`, `minio.username`, `minio.password`
- `postgres.host`, `postgres.port`, `postgres.username`, `postgres.password`, `postgres.dbname`
- `redis.host`, `redis.port`, `redis.username`, `redis.password`
- `websocket.host`, `websocket.port`
- `http.host`, `http.port`

See [`examples/config.json`](examples/config.json) for a working template.

## Database Setup
> [!WARNING]
>
> The script does not work YET, and it will be updated in the near future~

The SQL files in `scripts/` define the schema and related database objects.
To initialize a database, set `DB_URL` and run:

```bash
./scripts/init_db.sh
```

The script creates the required extensions and applies every `*.sql` file in the `scripts/` directory.

## Running Locally

One straightforward setup is:

1. Start the supporting services with Docker Compose.
2. Initialize the Postgres schema with `scripts/init_db.sh`.
3. Start the Rust server with a config file such as `examples/config.json`.

The example config uses:

- WebSocket on `127.0.0.1:8081`
- HTTP on `127.0.0.1:3000`
- Postgres on `localhost:5431`
- Redis on `localhost:6379`
- MinIO on `http://127.0.0.1:9000`

## HTTP API

Currently the HTTP server exposes:

- `POST /upload`

Requests must include a bearer token. The endpoint accepts multipart form data with:

- `server_id`
- `space_id`
- `file`

Uploads are stored in object storage, and the server records a file message in the database.

## WebSocket API

The WebSocket server is the primary API surface. It supports commands for:

- account management
- server and space lifecycle operations
- messaging and direct messaging
- friend request workflows
- moderation and ban lists
- subscription-based notifications

Request and response types are defined in `src/messages/` and `src/websocket.rs`.

> [!NOTE]
> 
> Other functionalities will be added in the future

## Status

This project is functional but still evolving. There are TODOs around permissions, naming cleanup, and additional tests.
