"""
Where HomeHarvest is running, and the few things that differ there.

On CPython HomeHarvest uses threads (parallel pages, detail batches, parsing) and httpx2.
Under Pyodide (WebAssembly, e.g. Cloudflare Python Workers), ``sys.platform`` is
``"emscripten"``: threads can't start, so everything runs one step at a time on the
calling thread, and HTTP goes through the JavaScript ``fetch`` API (see ``http.py``).
"""

from __future__ import annotations

import sys
import time
from concurrent.futures import Executor, Future, ThreadPoolExecutor

#: True under Pyodide / WebAssembly (Cloudflare Python Workers, the browser)
IN_PYODIDE = sys.platform == "emscripten"


class InlineExecutor(Executor):
    """An Executor that runs each call right away on the calling thread (no threads).

    ``submit`` returns a Future that is already done, so ``as_completed``, ``map`` and
    ``future.result()`` behave as with a thread pool that ran the calls in order.
    """

    def submit(self, fn, /, *args, **kwargs) -> Future:
        future: Future = Future()
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:  # stored on the Future, raised by .result(), as a pool does
            future.set_exception(exc)
        else:
            future.set_result(result)
        return future


def new_executor(max_workers: int) -> Executor:
    """A thread pool on CPython; an InlineExecutor where threads can't start (Pyodide)."""
    if IN_PYODIDE:
        return InlineExecutor()
    return ThreadPoolExecutor(max_workers=max_workers)


def sleep(seconds: float) -> None:
    """Wait without spinning the CPU.

    Under Pyodide with JSPI (a Cloudflare Python Worker request), wait on a JS timer
    through ``pyodide.ffi.run_sync``; elsewhere ``time.sleep``.
    """
    if seconds <= 0:
        return
    if IN_PYODIDE:
        try:
            import asyncio
            from pyodide.ffi import can_run_sync, run_sync
        except ImportError:  # pragma: no cover - very old Pyodide
            pass
        else:
            if can_run_sync():
                run_sync(asyncio.sleep(seconds))
                return
    time.sleep(seconds)
