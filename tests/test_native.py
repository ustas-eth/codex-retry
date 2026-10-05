"""Opt-in real Codex, temporary home, and local synthetic model. No credentials."""

import asyncio
import contextlib
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from codex_retry.recovery import recover, snapshot
from codex_retry.rpc import AppServer, RPCError
from codex_retry.runner import Runner


class Model(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        self.server.requests.append(
            json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        )
        if len(self.server.requests) == 1:
            events = [
                {
                    "type": "response.failed",
                    "response": {
                        "id": "synthetic-response",
                        "error": {
                            "code": "server_is_overloaded",
                            "message": "Synthetic model capacity failure",
                        },
                    },
                }
            ]
        else:
            events = [
                {"type": "response.created", "response": {"id": "synthetic-success"}},
                {
                    "type": "response.output_item.done",
                    "item": {
                        "type": "message",
                        "id": "synthetic-message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "OK"}],
                    },
                },
                {
                    "type": "response.completed",
                    "response": {
                        "id": "synthetic-success",
                        "usage": {
                            "input_tokens": 10,
                            "output_tokens": 1,
                            "total_tokens": 11,
                        },
                    },
                },
            ]
        payload = "".join("data: " + json.dumps(event) + "\n\n" for event in events).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class Native:
    def __init__(self, home):
        self.home = home
        self.socket = home / "app.sock"

    async def __aenter__(self):
        self.log = (self.home / "server.log").open("a")
        self.process = await asyncio.create_subprocess_exec(
            os.environ["CODEX_RETRY_TEST_BINARY"],
            "app-server",
            "--listen",
            f"unix://{self.socket}",
            env=os.environ | {"CODEX_HOME": str(self.home)},
            cwd=self.home,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=self.log,
            stderr=self.log,
        )
        try:
            for _ in range(300):
                if self.process.returncode is not None:
                    raise RuntimeError((self.home / "server.log").read_text())
                if self.socket.exists():
                    return self
                await asyncio.sleep(0.05)
            raise TimeoutError("temporary app-server did not start")
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def __aexit__(self, *_):
        if self.process.returncode is None:
            self.process.terminate()
        try:
            await asyncio.wait_for(self.process.wait(), 10)
        except TimeoutError:
            self.process.kill()
            await self.process.wait()
        self.log.close()


@unittest.skipUnless(os.environ.get("CODEX_RETRY_TEST_BINARY"), "opt-in isolated real Codex")
class NativeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="codex-retry-native-")
        self.home = Path(self.directory.name)
        self.backend = ThreadingHTTPServer(("127.0.0.1", 0), Model)
        self.backend.requests = []
        self.serving = threading.Thread(target=self.backend.serve_forever, daemon=True)
        self.serving.start()
        (self.home / "config.toml").write_text(
            'model = "gpt-6-luna"\nmodel_provider = "mock"\napproval_policy = "never"\n'
            'sandbox_mode = "read-only"\n[skills.bundled]\nenabled = false\n'
            "[features]\ngoals = true\n"
            '[model_providers.mock]\nname = "Local synthetic model"\nwire_api = "responses"\n'
            "supports_websockets = false\nrequires_openai_auth = false\n"
            f'base_url = "http://127.0.0.1:{self.backend.server_port}"\n'
        )

    async def asyncTearDown(self):
        await asyncio.to_thread(self.backend.shutdown)
        self.backend.server_close()
        self.serving.join()
        self.directory.cleanup()

    async def terminal(self, app, thread_id):
        for _ in range(200):
            try:
                current = await snapshot(app, thread_id, inspect_idle=True)
            except RPCError as exc:
                # A fresh thread's rollout may not yet be registered for paginated reads.
                if str(exc) != "list_turns is not supported yet":
                    raise
                await asyncio.sleep(0.05)
                continue
            if (
                current.status != "active"
                and current.turn
                and current.turn["status"] in {"failed", "completed"}
            ):
                return current
            await asyncio.sleep(0.05)
        self.fail("synthetic turn did not settle")

    async def create_failed(self, app, *, goal=False, history_mode="paginated"):
        result = await app.request(
            "thread/start",
            {
                "cwd": str(self.home),
                "persistExtendedHistory": True,
                "historyMode": history_mode,
            },
        )
        thread_id = result["thread"]["id"]
        if goal:
            await app.request(
                "thread/goal/set",
                {"threadId": thread_id, "objective": "Return OK.", "tokenBudget": 1000000},
            )
        else:
            await app.request(
                "turn/start",
                {
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "Synthetic fixture. Reply OK."}],
                },
            )
        current = await self.terminal(app, thread_id)
        self.assertTrue(current.capacity_failed, current)
        return thread_id, current.turn_id

    async def test_server_wide_runner_recovers_without_new_user_input(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.create_failed(app)
            runner = Runner(delay=0.01)
            task = asyncio.create_task(runner.serve(app, scan_interval=0.05))
            try:
                for _ in range(200):
                    current = await snapshot(app, thread_id)
                    if (
                        current.turn_id != old
                        and current.turn
                        and current.turn["status"] == "completed"
                    ):
                        break
                    await asyncio.sleep(0.05)
                self.assertEqual(current.turn["status"], "completed", current)
                self.assertEqual(len(self.backend.requests), 2)
                self.assertEqual(
                    self.backend.requests[0]["model"], self.backend.requests[1]["model"]
                )

                def users(request):
                    return [item for item in request["input"] if item.get("role") == "user"]

                self.assertEqual(users(self.backend.requests[0]), users(self.backend.requests[1]))
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def test_cold_blocked_goal_is_woken_once_without_goal_editing(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.create_failed(app, goal=True)
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            self.assertEqual((await snapshot(app, thread_id)).status, "notLoaded")
            result = await recover(app, thread_id, old)
            self.assertEqual(result["outcome"], "started")
            final = await self.terminal(app, thread_id)
            self.assertEqual(final.turn["status"], "completed")
            self.assertEqual(len(self.backend.requests), 2)
            objective = await app.request("thread/goal/get", {"threadId": thread_id})
            self.assertEqual(objective["goal"]["status"], "blocked")

    async def test_cold_active_goal_resume_is_not_started_twice(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.create_failed(app, goal=True)
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            # Setting a goal on a cold thread persists it without loading the thread.
            # A small budget ends the local mock's continuation after one success.
            await app.request(
                "thread/goal/set", {"threadId": thread_id, "status": "active", "tokenBudget": 1}
            )
            self.assertEqual((await snapshot(app, thread_id)).status, "notLoaded")
            result = await recover(app, thread_id, old)
            self.assertEqual(result["outcome"], "resumed")
            final = await self.terminal(app, thread_id)
            self.assertEqual(final.turn["status"], "completed")
            self.assertEqual(len(self.backend.requests), 2)

    async def test_legacy_failure_recovers_without_history_conversion(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.create_failed(app, history_mode="legacy")
            result = await recover(app, thread_id, old)
            self.assertEqual(result["outcome"], "started")
            final = await self.terminal(app, thread_id)
            self.assertEqual(final.turn["status"], "completed")
            thread = (
                await app.request("thread/read", {"threadId": thread_id, "includeTurns": False})
            )["thread"]
            self.assertEqual(thread["historyMode"], "legacy")
            self.assertEqual(len(self.backend.requests), 2)
