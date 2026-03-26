# noisypackets

A small Python HTTP proxy that wraps any HTTP server process and injects SSE keepalive comments into streaming responses to prevent intermediate proxies (e.g. Cloudflare's 2-minute no-data timeout) from closing the connection.

## Problem

When a reverse proxy like [llama-swap](https://github.com/mostlygeek/llama-swap) points at a backend server, slow streaming responses can go silent for more than 2 minutes — causing Cloudflare (or other intermediate proxies) to kill the connection. Patching the backend or the reverse proxy is impractical.

## Solution

noisypackets sits between your reverse proxy and the backend:

```
llama-swap → noisypackets (:8081) → llama-server (:8080)
```

It:
1. Starts the backend server as a subprocess
2. Listens on a configurable port
3. Proxies all HTTP traffic transparently to the backend
4. For SSE streaming responses, injects a `: keepalive\n\n` comment whenever no data arrives within the keepalive interval

No patches needed. The reverse proxy thinks it's talking to the backend directly.

## Installation

```bash
pip install -r requirements.txt
```

Requires Python 3.8+.

## Usage

```
noisypackets.py [--listen-port PORT] [--backend-port PORT] [--keepalive SECONDS] <command and args...>
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--listen-port` | `8081` | Port this proxy listens on |
| `--backend-port` | auto | Port the backend listens on (auto-detected from `--port` in the command args) |
| `--keepalive` | `15.0` | Seconds of stream silence before injecting a keepalive comment. Set to `0` to disable. |

### Example

llama-swap's model config points to port 8081. llama-server normally runs on 8080:

```bash
noisypackets.py --listen-port 8081 --keepalive 15 \
    llama-server --port 8080 -m /models/my-model.gguf
```

noisypackets starts llama-server internally, waits for it to be healthy, then proxies `8081 → 8080`.

## How it works

- **SSE detection** — checks `Content-Type: text/event-stream` on the upstream response
- **Keepalive injection** — uses `asyncio.wait_for(read_chunk(), timeout=interval)`; on timeout, writes `: keepalive\n\n` and resumes
- **Non-SSE responses** — passed through completely unchanged (body, status, headers)
- **Subprocess lifecycle** — polls `GET /health` until the backend is ready (60s timeout), sends SIGTERM on shutdown
- **Backend failure** — returns 503 if the backend process has exited unexpectedly

The `: keepalive\n\n` comment is a valid SSE comment per spec and is silently ignored by all clients (SillyTavern, OpenAI-compatible clients, etc.).

## What this is NOT

- Not a load balancer
- Not a model manager (llama-swap handles that)
- Not a replacement for llama-swap — it sits *between* llama-swap and llama-server
- Does not need to understand the SSE payload format — treats it as an opaque byte stream
