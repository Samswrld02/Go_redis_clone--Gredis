# Gredis

Gredis is a small Redis-inspired key-value server written in Go.

The main goal of this project is learning by building: understanding concurrency, shared state protection with mutexes, and clean code composition using patterns like factory construction and dependency injection.

## Why this project?

- Practice network programming with raw TCP sockets.
- Learn how concurrent client handling works in Go.
- Explore synchronization strategies for shared in-memory state.
- Improve code organization through composition over tight coupling.

## Current features

- TCP server (default: `:6379`)
- Basic in-memory key-value store
- Command parsing for:
	- `SET <key> <value>`
	- `GET <key>`
- Concurrent client handling via goroutines

## Project structure

```text
.
├── main.go              # App entrypoint
├── Client/
│   └── connection.go    # Client model
└── Server/
    ├── server.go        # TCP server and command handling
    └── db/
        └── db.go        # In-memory datastore
```

## Run locally

```bash
go run main.go
```

Then connect from another terminal:

```bash
nc localhost 6379
```

Try commands:

```text
SET name sam
GET name
```

## Learning focus (in progress)

- Add mutex-protected shared state for thread-safe reads/writes.
- Reduce coupling with better dependency injection between server and datastore.
- Introduce factory-style initialization for server and components.
- Expand command support and improve protocol parsing.
- Add tests for command parsing and datastore behavior.

## Notes

This is a learning project, not a production Redis replacement.
