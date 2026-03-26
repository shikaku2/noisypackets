# noisypackets

A small Python HTTP proxy that wraps any HTTP server process and injects SSE
keepalive comments into streaming responses to prevent intermediate proxies
(e.g. Cloudflare's 2-minute no-data timeout) from closing the connection.

General-purpose — not specific to llama-server or llama-swap.

## Problem

A reverse proxy (e.g. llama-swap) points at a backend server. During slow
streaming responses, no SSE data flows for >2 minutes and Cloudflare (or other
proxies) kill the connection. Patching the backend or the reverse proxy is
impractical.

## Solution

A standalone proxy that:
1. Starts the backend server as a subprocess (command + all args passed through)
2. Listens on a port that the reverse proxy points to instead of the backend directly
3. Proxies all HTTP traffic transparently to the backend
4. For SSE streaming responses (`Content-Type: text/event-stream`), injects
   `: keepalive\n\n` whenever no data arrives within the keepalive interval

No patches to anything. The reverse proxy thinks it's talking to the backend.
The backend is completely unmodified.

## SSE keepalive comment

```
: keepalive\n\n
```

Valid SSE comment per spec (RFC), silently ignored by all clients including
SillyTavern, OpenAI-compatible clients, etc.

---

## Usage

```
noisypackets.py [--listen-port PORT] [--keepalive SECONDS] <command and args...>
```

Example — llama-swap config points to port 8081, llama-server normally runs on 8080:

```
noisypackets.py --listen-port 8081 --keepalive 15 \
    llama-server --port 8080 -m /models/my-model.gguf ...
```

llama-swap's model entry uses port 8081. noisypackets starts llama-server
on 8080 internally and proxies 8081 → 8080.

---

## Architecture

- **Language**: Python 3, asyncio
- **HTTP proxy**: `aiohttp` (client) + `aiohttp.web` or raw asyncio (server side)
  - Alternative: `httpx` + `starlette` if aiohttp proves awkward
- **Subprocess**: `asyncio.create_subprocess_exec` with stderr forwarded to our
  stderr (so llama-server logs still appear)
- **SSE detection**: check `Content-Type: text/event-stream` on the upstream response
- **Keepalive injection**: use `asyncio.wait_for(read_next_chunk(), timeout=interval)`
  — on `TimeoutError`, write `: keepalive\n\n` and flush, then resume waiting

## Key design decisions

- Keepalive is driven by **stream silence** (no bytes from upstream for N seconds),
  not by watching llama-server stderr. Stream silence is the correct signal because
  that's exactly what Cloudflare detects.
- Non-SSE responses (JSON, health checks, etc.) pass through completely unchanged.
- The proxy must forward the upstream SSE headers unchanged before any keepalive
  can be sent (`Content-Type: text/event-stream`, `Cache-Control: no-cache`,
  `X-Accel-Buffering: no`, etc.).
- Keepalive interval default: **15 seconds** (well under Cloudflare's 100s idle
  timeout, comfortably under the 2-minute hard limit).
- Configurable via `--keepalive 0` to disable (pass-through only mode).

## Proxy behavior pseudocode

```python
async def proxy_request(request):
    async with session.request(method, upstream_url, headers=..., data=...) as upstream:
        if upstream.content_type == "text/event-stream":
            response = web.StreamResponse(headers=upstream.headers)
            await response.prepare(request)

            while True:
                try:
                    chunk = await asyncio.wait_for(
                        upstream.content.read(4096),
                        timeout=keepalive_interval
                    )
                    if not chunk:
                        break
                    await response.write(chunk)
                except asyncio.TimeoutError:
                    await response.write(b": keepalive\n\n")
            return response
        else:
            # non-streaming: pass through normally
            body = await upstream.read()
            return web.Response(status=upstream.status, headers=upstream.headers, body=body)
```

## Subprocess lifecycle

- On startup: launch llama-server subprocess, wait for it to be ready (poll
  `GET /health` until 200 or timeout).
- On shutdown (SIGTERM/SIGINT): send SIGTERM to subprocess, wait for it to exit.
- If subprocess dies unexpectedly: proxy returns 503, log the exit code.

## File layout

```
noisypackets/
  noisypackets.py   # single file — arg parsing, subprocess management, HTTP proxy
  requirements.txt  # aiohttp (and nothing else if possible)
```

---

## What this is NOT

- Not a load balancer
- Not a model manager (llama-swap handles that)
- Not a replacement for llama-swap — it sits *between* llama-swap and llama-server
- Does not need to understand the SSE payload format at all — it treats it as
  an opaque byte stream and only injects at silence boundaries
