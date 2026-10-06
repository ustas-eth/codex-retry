"""Small JSON-RPC client for an existing Codex app-server."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path

from websockets.asyncio.client import connect, unix_connect

from . import __version__


class RPCError(RuntimeError):
    def __init__(self, payload):
        self.payload = payload
        super().__init__(payload.get("message", "app-server rejected request"))


def unix_path(endpoint: str) -> str:
    raw = endpoint.removeprefix("unix://")
    if raw:
        return str(Path(raw).expanduser().absolute())
    home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
    return str(home / "app-server-control" / "app-server-control.sock")


class AppServer:
    def __init__(self, endpoint="unix://", timeout=30):
        self.endpoint, self.timeout = endpoint, timeout
        self.sequence = 0
        self.pending = {}
        self.ws = self.reader = None
        self.changed = asyncio.Event()
        self.dirty = set()
        self.archived = set()

    async def __aenter__(self):
        options = dict(compression=None, max_size=16 * 1024 * 1024, open_timeout=self.timeout)
        if self.endpoint.startswith("unix://"):
            self.ws = await unix_connect(
                unix_path(self.endpoint), uri="ws://localhost/rpc", **options
            )
        elif self.endpoint.startswith(("ws://", "wss://")):
            self.ws = await connect(self.endpoint, **options)
        else:
            raise ValueError("endpoint must use unix://, ws://, or wss://")
        self.reader = asyncio.create_task(self._receive())
        try:
            await self.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "codex_retry",
                        "title": "codex-retry",
                        "version": __version__,
                    },
                    "capabilities": {"experimentalApi": True},
                },
            )
            await self.ws.send(json.dumps({"method": "initialized", "params": {}}))
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, *_):
        if self.ws:
            await self.ws.close()
        if self.reader:
            self.reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.reader

    async def _receive(self):
        failure = RuntimeError("app-server connection closed")
        try:
            async for raw in self.ws:
                message = json.loads(raw)
                if not isinstance(message, dict):
                    raise RuntimeError("app-server returned a non-object message")
                if "method" in message:
                    # Leave UI/approval requests to the operator's client.
                    if message["method"] in {
                        "thread/status/changed",
                        "thread/goal/updated",
                        "thread/archived",
                        "thread/unarchived",
                    }:
                        thread_id = message.get("params", {}).get("threadId")
                        if isinstance(thread_id, str):
                            if message["method"] == "thread/archived":
                                self.archived.add(thread_id)
                            elif message["method"] == "thread/unarchived":
                                self.archived.discard(thread_id)
                            self.dirty.add(thread_id)
                            self.changed.set()
                    continue
                future = self.pending.get(message.get("id"))
                if future is not None and not future.done():
                    if "error" in message:
                        future.set_exception(RPCError(message["error"]))
                    elif "result" in message:
                        future.set_result(message["result"])
                    else:
                        future.set_exception(RuntimeError("app-server response has no result"))
        except Exception as exc:
            failure = exc
        finally:
            self.changed.set()
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(failure)

    async def request(self, method, params):
        if self.reader.done():
            raise RuntimeError("app-server connection closed")
        self.sequence += 1
        request_id = self.sequence
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        try:
            async with asyncio.timeout(self.timeout):
                await self.ws.send(
                    json.dumps({"id": request_id, "method": method, "params": params})
                )
                result = await future
            if not isinstance(result, dict):
                raise RuntimeError(f"invalid result for {method}")
            return result
        except TimeoutError as exc:
            raise TimeoutError(f"timed out waiting for app-server method {method}") from exc
        finally:
            self.pending.pop(request_id, None)
            if not future.done():
                future.cancel()
