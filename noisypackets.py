#!/usr/bin/env python3
"""
noisypackets.py — HTTP proxy that injects SSE keepalive comments into streaming
responses to prevent intermediate proxies (e.g. Cloudflare) from timing out.

Usage:
    noisypackets.py [--listen-port PORT] [--keepalive SECONDS] <command and args...>

Example:
    noisypackets.py --listen-port 8081 --keepalive 15 \
        llama-server --port 8080 -m /models/my-model.gguf
"""

import argparse
import asyncio
import logging
import signal
import sys

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [noisypackets] %(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("noisypackets")

KEEPALIVE_COMMENT = b": keepalive\n\n"
HEALTH_POLL_INTERVAL = 1.0   # seconds between /health polls
HEALTH_POLL_TIMEOUT = 60.0   # total seconds to wait for backend to become ready
READ_CHUNK = 4096


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="SSE keepalive proxy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--listen-port", type=int, default=8081,
        help="Port this proxy listens on (default: 8081)",
    )
    parser.add_argument(
        "--backend-port", type=int, default=None,
        help="Port the backend listens on. Auto-detected from --port in the "
             "command args if not specified.",
    )
    parser.add_argument(
        "--keepalive", type=float, default=15.0,
        help="Seconds of stream silence before injecting a keepalive comment "
             "(default: 15). Set to 0 to disable.",
    )
    parser.add_argument(
        "command", nargs=argparse.REMAINDER,
        help="Backend command and all its arguments",
    )
    return parser.parse_args(argv)


def detect_backend_port(command_args):
    """Pull --port <N> out of the backend command args if present."""
    for i, arg in enumerate(command_args):
        if arg == "--port" and i + 1 < len(command_args):
            try:
                return int(command_args[i + 1])
            except ValueError:
                pass
        if arg.startswith("--port="):
            try:
                return int(arg.split("=", 1)[1])
            except ValueError:
                pass
    return None


# ---------------------------------------------------------------------------
# Subprocess management
# ---------------------------------------------------------------------------

class BackendProcess:
    def __init__(self, command):
        self.command = command
        self.proc = None

    async def start(self):
        log.info("Starting backend: %s", " ".join(self.command))
        self.proc = await asyncio.create_subprocess_exec(
            *self.command,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=None,   # inherit our stdout
            stderr=None,   # inherit our stderr
        )
        log.info("Backend PID %d", self.proc.pid)

    async def wait_ready(self, backend_url):
        deadline = asyncio.get_event_loop().time() + HEALTH_POLL_TIMEOUT
        health_url = backend_url.rstrip("/") + "/health"
        log.info("Waiting for backend at %s …", health_url)
        connector = aiohttp.TCPConnector()
        async with aiohttp.ClientSession(connector=connector) as session:
            while asyncio.get_event_loop().time() < deadline:
                if self.proc.returncode is not None:
                    raise RuntimeError(
                        f"Backend exited prematurely (code {self.proc.returncode})"
                    )
                try:
                    async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=2)) as r:
                        if r.status == 200:
                            log.info("Backend is ready.")
                            return
                except Exception:
                    pass
                await asyncio.sleep(HEALTH_POLL_INTERVAL)
        raise TimeoutError(f"Backend not ready after {HEALTH_POLL_TIMEOUT}s")

    async def stop(self):
        if self.proc and self.proc.returncode is None:
            log.info("Sending SIGTERM to backend PID %d", self.proc.pid)
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                log.warning("Backend did not exit; sending SIGKILL")
                self.proc.kill()
                await self.proc.wait()
            log.info("Backend exited with code %d", self.proc.returncode)


# ---------------------------------------------------------------------------
# HTTP proxy
# ---------------------------------------------------------------------------

def build_forwarded_headers(request):
    """Copy request headers, stripping hop-by-hop headers."""
    hop_by_hop = {
        "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailers", "transfer-encoding", "upgrade",
        "host",  # aiohttp sets this automatically
    }
    return {k: v for k, v in request.headers.items() if k.lower() not in hop_by_hop}


def build_response_headers(upstream_headers):
    """Copy upstream response headers, stripping hop-by-hop headers."""
    hop_by_hop = {
        "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailers", "transfer-encoding", "upgrade",
        "content-length",  # we may change the body length with keepalives
    }
    return {k: v for k, v in upstream_headers.items() if k.lower() not in hop_by_hop}


class ProxyHandler:
    def __init__(self, backend_url, keepalive_interval, backend_proc):
        self.backend_url = backend_url.rstrip("/")
        self.keepalive_interval = keepalive_interval if keepalive_interval > 0 else None
        self.backend_proc = backend_proc
        self._session = None

    def session(self):
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=0)
            timeout = aiohttp.ClientTimeout(
                connect=10,
                sock_connect=10,
                sock_read=None,  # we handle read timeouts ourselves for SSE
            )
            self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def handle(self, request):
        # If backend has died, return 503 immediately
        proc = self.backend_proc.proc
        if proc is not None and proc.returncode is not None:
            log.error("Backend is down (exit code %d)", proc.returncode)
            return web.Response(status=503, text="Backend process has exited")

        target_url = self.backend_url + request.path_qs
        req_headers = build_forwarded_headers(request)

        try:
            body = await request.read()
        except Exception as exc:
            log.warning("Failed to read request body: %s", exc)
            return web.Response(status=400, text="Bad request")

        try:
            upstream_resp = await self.session().request(
                method=request.method,
                url=target_url,
                headers=req_headers,
                data=body if body else None,
                allow_redirects=False,
            )
        except aiohttp.ClientConnectorError as exc:
            log.error("Cannot connect to backend: %s", exc)
            return web.Response(status=502, text="Cannot connect to backend")
        except Exception as exc:
            log.error("Upstream request failed: %s", exc)
            return web.Response(status=502, text="Bad gateway")

        is_sse = "text/event-stream" in upstream_resp.headers.get("Content-Type", "")

        resp_headers = build_response_headers(upstream_resp.headers)

        if is_sse:
            return await self._stream_sse(request, upstream_resp, resp_headers)
        else:
            return await self._passthrough(upstream_resp, resp_headers)

    async def _passthrough(self, upstream_resp, resp_headers):
        try:
            body = await upstream_resp.read()
        except Exception as exc:
            log.warning("Error reading upstream body: %s", exc)
            return web.Response(status=502, text="Error reading upstream response")
        return web.Response(
            status=upstream_resp.status,
            headers=resp_headers,
            body=body,
        )

    async def _stream_sse(self, request, upstream_resp, resp_headers):
        response = web.StreamResponse(
            status=upstream_resp.status,
            headers=resp_headers,
        )
        # Ensure chunked transfer so the client receives data as it arrives
        response.enable_chunked_encoding()
        try:
            await response.prepare(request)
        except Exception as exc:
            log.warning("Failed to prepare SSE stream response: %s", exc)
            return response

        try:
            while True:
                try:
                    if self.keepalive_interval:
                        chunk = await asyncio.wait_for(
                            upstream_resp.content.read(READ_CHUNK),
                            timeout=self.keepalive_interval,
                        )
                    else:
                        chunk = await upstream_resp.content.read(READ_CHUNK)
                except asyncio.TimeoutError:
                    try:
                        await response.write(KEEPALIVE_COMMENT)
                    except Exception:
                        break
                    continue

                if not chunk:
                    break

                try:
                    await response.write(chunk)
                except Exception:
                    break
        finally:
            upstream_resp.release()
            try:
                await response.write_eof()
            except Exception:
                pass

        return response


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(args):
    if not args.command:
        print("Error: no backend command specified.", file=sys.stderr)
        sys.exit(1)

    backend_port = args.backend_port or detect_backend_port(args.command)
    if backend_port is None:
        print(
            "Error: cannot detect backend port. Pass --backend-port or include "
            "--port <N> in the backend command.",
            file=sys.stderr,
        )
        sys.exit(1)

    backend_url = f"http://127.0.0.1:{backend_port}"

    backend = BackendProcess(args.command)
    await backend.start()

    try:
        await backend.wait_ready(backend_url)
    except (TimeoutError, RuntimeError) as exc:
        log.error("%s", exc)
        await backend.stop()
        sys.exit(1)

    handler = ProxyHandler(backend_url, args.keepalive, backend)

    app = web.Application()
    app.router.add_route("*", "/{path_info:.*}", handler.handle)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", args.listen_port)
    await site.start()
    log.info(
        "Proxy listening on :%d → %s (keepalive=%.1fs)",
        args.listen_port, backend_url, args.keepalive,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        log.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    await stop_event.wait()

    log.info("Shutting down…")
    await runner.cleanup()
    await handler.close()
    await backend.stop()


if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        pass
