"""
Small HTTP transport used by every scraper request.

The rest of HomeHarvest only sees ``HttpClient.post`` and ``HttpResponse``, so the
HTTP library can change without touching the scrapers. The current backend is httpx2
(pydantic's maintained fork of httpx) with HTTP/2: one pooled connection carries concurrent requests (multiplexed), so
parallel pages and detail batches share one TLS connection instead of opening one each.

Under Pyodide (Cloudflare Python Workers) there are no threads and no sockets, so
``HttpClient`` sends each request through the JavaScript ``fetch`` API instead
(``FetchTransport``), blocking on it with ``pyodide.ffi.run_sync`` (JSPI). The rest of
HomeHarvest doesn't change.

Network failures are raised as ``homeharvest.exceptions.NetworkError`` and HTTP error
statuses (from ``raise_for_status``) as ``homeharvest.exceptions.HTTPStatusError``.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Awaitable, Callable, Mapping

from ...exceptions import NetworkError, HTTPStatusError
from .runtime import IN_PYODIDE

if not IN_PYODIDE:  #: httpx2 isn't installed under Pyodide (its sockets and threads don't work there)
    import httpx2

#: most requests one search keeps in flight at once (pages + detail batches together)
DEFAULT_MAX_IN_FLIGHT = 4  #: the same peak as 0.9.0 (4 parallel pages), to avoid IP bans


class HttpResponse:
    """The parts of a response HomeHarvest uses (a tiny subset of requests.Response)."""

    __slots__ = ("status_code", "headers", "content", "http_version", "url")

    def __init__(self, status_code: int, headers: Mapping[str, str], content: bytes, http_version: str = "", url: str = ""):
        self.status_code = status_code
        self.headers = headers
        self.content = content
        self.http_version = http_version
        self.url = url

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPStatusError(f"HTTP {self.status_code} from {self.url}", response=self)


class Headers(dict):
    """Response headers with case-insensitive lookups (fetch gives lowercase names)."""

    def __init__(self, items=()):
        super().__init__((str(key).lower(), value) for key, value in dict(items).items())

    def __getitem__(self, key):
        return super().__getitem__(key.lower())

    def __contains__(self, key):
        return super().__contains__(key.lower() if isinstance(key, str) else key)

    def get(self, key, default=None):
        return super().get(key.lower() if isinstance(key, str) else key, default)


#: (url, method=, headers=, body=, timeout=) -> HttpResponse
FetchFunction = Callable[..., Awaitable[HttpResponse]]


async def js_fetch(url: str, *, method: str, headers: Mapping[str, str] | None, body, timeout: float) -> HttpResponse:
    """One request through the JavaScript fetch API (Pyodide only)."""
    from js import AbortSignal, Object, fetch  # type: ignore[import-not-found]
    from pyodide.ffi import JsException, to_js  # type: ignore[import-not-found]

    if isinstance(body, bytes):
        body = body.decode("utf-8")
    options = to_js(
        {
            "method": method,
            "headers": dict(headers or {}),
            "body": body,
            "signal": AbortSignal.timeout(int(timeout * 1000)),
        },
        dict_converter=Object.fromEntries,
    )
    try:
        response = await fetch(url, options)
        content = (await response.arrayBuffer()).to_bytes()
    except JsException as exc:  #: DNS / connection / TLS errors, timeouts (AbortSignal)
        raise NetworkError(f"fetch failed: {exc}") from exc
    return HttpResponse(
        status_code=response.status,
        headers=Headers((key, value) for key, value in response.headers.entries()),
        content=content,
        url=str(response.url or url),
    )


class FetchTransport:
    """Sends requests with fetch and waits for each one (no threads, no sockets).

    The default is the JavaScript fetch API under Pyodide, which needs JSPI: call
    HomeHarvest from inside a request handler of a Cloudflare Python Worker (or any
    Pyodide code that ``pyodide.ffi.can_run_sync()`` allows). ``fetch`` and ``run_sync``
    can be swapped, e.g. to test this path on CPython.
    """

    def __init__(
        self,
        timeout: float = 60.0,
        fetch: FetchFunction | None = None,
        run_sync: Callable[[Awaitable], object] | None = None,
    ):
        self.timeout = timeout
        self._fetch = fetch or js_fetch
        self._run_sync = run_sync

    def _blocking(self) -> Callable[[Awaitable], object]:
        if self._run_sync is not None:
            return self._run_sync
        from pyodide.ffi import can_run_sync, run_sync  # type: ignore[import-not-found]

        if not can_run_sync():
            raise RuntimeError(
                "HomeHarvest under Pyodide needs JSPI to wait for fetch. Call it from inside a request "
                "handler (Cloudflare Python Workers), not at import time."
            )
        return run_sync

    def post(self, url: str, *, data: str | bytes, headers: Mapping[str, str] | None = None) -> HttpResponse:
        return self._blocking()(self._fetch(url, method="POST", headers=headers, body=data, timeout=self.timeout))


class HttpClient:
    """Thread-safe pooled HTTP client with a cap on requests in flight.

    One instance is shared by everything a single search does (pages, detail batches),
    so ``max_in_flight`` bounds the total fan-out no matter how many threads call it.

    Requests run on httpx2's async HTTP/2 client in one private event-loop thread; callers
    use the blocking ``post``. (The sync client, in httpx and httpx2 2.16 alike, shares one
    HTTP/2 socket between threads unsafely: under load reads fail with "Resource
    temporarily unavailable".)

    Under Pyodide (or with ``transport=``) requests go through a ``FetchTransport``
    instead: no thread, no httpx2.
    """

    def __init__(
        self,
        proxy: str | None = None,
        timeout: float = 60.0,
        max_in_flight: int = DEFAULT_MAX_IN_FLIGHT,
        reuse_connections: bool | None = None,
        transport: FetchTransport | None = None,
    ):
        if transport is None and IN_PYODIDE:
            if proxy:
                raise ValueError("Proxies aren't supported under Pyodide: requests go through the JavaScript fetch API.")
            transport = FetchTransport(timeout=timeout)
        self.transport = transport
        #: "host:port" / "user:pass@host:port" without a scheme mean an HTTP proxy (as with requests)
        self.proxy = proxy if not proxy or "://" in proxy else f"http://{proxy}"
        #: default: pool without a proxy; with one, a new connection (tunnel) per request,
        #: as before 0.9.1, so rotating proxies still give each request its own exit IP
        self.reuse_connections = proxy is None if reuse_connections is None else reuse_connections
        self.timeout = timeout
        self.max_in_flight = max_in_flight
        self._slots = threading.BoundedSemaphore(max_in_flight)
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._client = None  #: httpx2.AsyncClient, started on first use
        self._pause_until = 0.0

    def _new_client(self) -> "httpx2.AsyncClient":
        return httpx2.AsyncClient(
            http2=True,
            proxy=self.proxy,
            timeout=self.timeout,
            limits=httpx2.Limits(max_connections=self.max_in_flight, max_keepalive_connections=self.max_in_flight),
        )

    def _start(self) -> tuple[asyncio.AbstractEventLoop, "httpx2.AsyncClient"]:
        with self._lock:
            if self._loop is None:
                loop = asyncio.new_event_loop()
                thread = threading.Thread(target=loop.run_forever, name="homeharvest-http", daemon=True)
                thread.start()

                async def make_client() -> "httpx2.AsyncClient":
                    return self._new_client()

                try:
                    client = asyncio.run_coroutine_threadsafe(make_client(), loop).result()
                except BaseException:
                    loop.call_soon_threadsafe(loop.stop)
                    thread.join()
                    loop.close()
                    raise
                self._loop, self._thread, self._client = loop, thread, client
            return self._loop, self._client

    async def _post(self, client: "httpx2.AsyncClient", url: str, data, headers, fresh: bool) -> HttpResponse:
        if fresh:  #: one-off connection (with a rotating proxy: a new exit IP)
            async with self._new_client() as one_off:
                response = await one_off.post(url, content=data, headers=headers)
        else:
            response = await client.post(url, content=data, headers=headers)
        return HttpResponse(
            status_code=response.status_code,
            headers=response.headers,
            content=response.content,
            http_version=response.http_version,
            url=str(response.url),
        )

    def post(
        self, url: str, *, data: str | bytes, headers: Mapping[str, str] | None = None, fresh_connection: bool = False
    ) -> HttpResponse:
        """POST and return the whole response. fresh_connection=True skips the pooled connection."""
        if self.transport is not None:  #: fetch (Pyodide): every request stands alone, nothing pooled
            with self._slots:
                return self.transport.post(url, data=data, headers=headers)
        loop, client = self._start()
        with self._slots:
            fresh = fresh_connection or not self.reuse_connections
            future = asyncio.run_coroutine_threadsafe(self._post(client, url, data, headers, fresh), loop)
            try:
                return future.result()
            except httpx2.RequestError as exc:  #: connect / read / timeout / protocol / proxy / decoding errors
                raise NetworkError(f"{type(exc).__name__}: {exc}") from exc

    # ---- shared back-off: when one request is told to slow down (429), every thread waits

    def pause(self, seconds: float) -> None:
        with self._lock:
            self._pause_until = max(self._pause_until, time.monotonic() + seconds)

    def pause_remaining(self) -> float:
        return max(0.0, self._pause_until - time.monotonic())

    def close(self) -> None:
        """Close the connection and stop the loop thread. The client restarts if used again."""
        with self._lock:
            loop, thread, client = self._loop, self._thread, self._client
            self._loop = self._thread = self._client = None
        if loop is None:
            return
        try:
            asyncio.run_coroutine_threadsafe(client.aclose(), loop).result(timeout=10)
        except Exception:  # closing is best effort
            pass
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=10)
        if not thread.is_alive():
            loop.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
