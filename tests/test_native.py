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
        if len(self.server.requests) in self.server.fail_at:
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
        elif len(self.server.requests) in getattr(self.server, "block_at", set()):
            events = [
                {"type": "response.created", "response": {"id": "synthetic-agent-block"}},
                {
                    "type": "response.output_item.done",
                    "item": {
                        "type": "function_call",
                        "id": "synthetic-goal-tool",
                        "call_id": "synthetic-goal-call",
                        "name": "update_goal",
                        "arguments": '{"status":"blocked"}',
                    },
                },
                {
                    "type": "response.completed",
                    "response": {
                        "id": "synthetic-agent-block",
                        "usage": {"input_tokens": 10, "output_tokens": 1, "total_tokens": 11},
                    },
                },
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
        self.backend.fail_at = {1}
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
                current = await snapshot(app, thread_id)
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

    async def create_failed(self, app, *, goal=False, budget=1000000, history_mode="paginated"):
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
                {"threadId": thread_id, "objective": "Return OK.", "tokenBudget": budget},
            )
        else:
            await app.request(
                "turn/start",
                {
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "Synthetic fixture. Reply OK."}],
                },
            )
        for _ in range(200):
            current = await self.terminal(app, thread_id)
            if current.capacity_failed:
                return thread_id, current.turn_id
            await asyncio.sleep(0.05)
        self.fail(f"synthetic capacity error was not observed: {current}")

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

    async def test_cold_blocked_goal_restores_sustained_work_through_saved_discovery(self):
        self.backend.fail_at = {2}
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, _ = await self.create_failed(app, goal=True, budget=40)
            before = (await app.request("thread/goal/get", {"threadId": thread_id}))["goal"]
            self.assertEqual(before["status"], "blocked")
            self.assertEqual(before["tokensUsed"], 11)
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            self.assertEqual((await snapshot(app, thread_id)).status, "notLoaded")
            commands = []
            original_request = app.request

            async def recorded_request(method, params):
                if method in {"thread/resume", "thread/goal/set", "turn/start"}:
                    commands.append((method, params.copy()))
                return await original_request(method, params)

            app.request = recorded_request
            runner = Runner(delay=0.01)
            task = asyncio.create_task(runner.serve(app, scan_interval=0.05))
            try:
                for _ in range(200):
                    objective = (await app.request("thread/goal/get", {"threadId": thread_id}))[
                        "goal"
                    ]
                    if objective["status"] == "budgetLimited":
                        break
                    await asyncio.sleep(0.05)
                self.assertEqual(objective["status"], "budgetLimited")
                self.assertEqual(objective["objective"], before["objective"])
                self.assertEqual(objective["tokenBudget"], 40)
                self.assertEqual(objective["tokensUsed"], before["tokensUsed"] + 33)
                self.assertEqual(objective["createdAt"], before["createdAt"])
                self.assertGreaterEqual(objective["timeUsedSeconds"], before["timeUsedSeconds"])
                self.assertEqual(len(self.backend.requests), 5)
                # Native goal reminders change between turns. Verify our actual
                # control requests rather than mistaking those for user input.
                self.assertEqual(
                    commands,
                    [
                        ("thread/resume", {"threadId": thread_id, "excludeTurns": True}),
                        ("thread/goal/set", {"threadId": thread_id, "status": "active"}),
                    ],
                )
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def test_loaded_blocked_goal_reactivation_starts_only_one_turn(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.create_failed(app, goal=True, budget=1)
            result = await recover(app, thread_id, old)
            self.assertTrue(result["goalResumed"])
            final = await self.terminal(app, thread_id)
            self.assertEqual(final.turn["status"], "completed")
            self.assertEqual(len(self.backend.requests), 2)

    async def agent_block_then_capacity(self, app):
        self.backend.block_at, self.backend.fail_at = {1}, {2}
        thread_id, old = await self.create_failed(app, goal=True, budget=22)
        outputs = [
            item["output"]
            for item in self.backend.requests[1]["input"]
            if item.get("type") == "function_call_output"
            and item.get("call_id") == "synthetic-goal-call"
        ]
        self.assertEqual(len(outputs), 1)
        self.assertEqual(json.loads(outputs[0])["goal"]["status"], "blocked")
        return thread_id, old

    async def test_best_effort_default_resumes_agent_block_followed_by_capacity_failure(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.agent_block_then_capacity(app)
            self.assertTrue((await recover(app, thread_id, old))["goalResumed"])
            final = await self.terminal(app, thread_id)
            self.assertEqual(final.turn["status"], "completed")
            self.assertEqual(len(self.backend.requests), 3)

    async def test_disabled_goal_recovery_preserves_agent_block_after_restart(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.agent_block_then_capacity(app)
            before = (await app.request("thread/goal/get", {"threadId": thread_id}))["goal"]
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            events = []
            runner = Runner(
                delay=0.001,
                resume_blocked_goals=False,
                emit=lambda event, **fields: events.append((event, fields)),
            )
            await runner.serve(app, dry_run=True)
            self.assertEqual(events[-1][0], "wouldRetry")
            self.assertEqual(events[-1][1]["goalStatus"], "blocked")
            self.assertIsNone(events[-1][1]["goalAction"])
            self.assertEqual((await snapshot(app, thread_id)).status, "notLoaded")
            result = await recover(app, thread_id, old, resume_blocked_goals=False)
            self.assertEqual(result["outcome"], "started")
            final = await self.terminal(app, thread_id)
            self.assertEqual(final.turn["status"], "completed")
            after = (await app.request("thread/goal/get", {"threadId": thread_id}))["goal"]
            for field in ["status", "objective", "tokenBudget", "tokensUsed", "createdAt"]:
                self.assertEqual(after[field], before[field])
            self.assertEqual(len(self.backend.requests), 3)

    async def test_goal_retries_repeated_capacity_failures_until_service_recovers(self):
        self.backend.fail_at = {1, 2}
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, _ = await self.create_failed(app, goal=True, budget=1)
            events = []
            runner = Runner(delay=0.01, emit=lambda event, **fields: events.append((event, fields)))
            task = asyncio.create_task(runner.serve(app, scan_interval=0.05))
            try:
                for _ in range(200):
                    objective = (await app.request("thread/goal/get", {"threadId": thread_id}))[
                        "goal"
                    ]
                    if objective["status"] == "budgetLimited":
                        break
                    await asyncio.sleep(0.05)
                self.assertEqual(objective["status"], "budgetLimited")
                self.assertEqual(len(self.backend.requests), 3)
                resumed = [fields for event, fields in events if event == "resumed"]
                self.assertEqual(len(resumed), 2)
                self.assertTrue(all(fields["goalResumed"] for fields in resumed))
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def test_paused_goal_preserved_while_cold_capacity_failure_recovers(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.create_failed(app, goal=True)
            await app.request("thread/goal/set", {"threadId": thread_id, "status": "paused"})
            before = (await app.request("thread/goal/get", {"threadId": thread_id}))["goal"]
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            events = []
            runner = Runner(emit=lambda event, **fields: events.append((event, fields)))
            await runner.serve(app, dry_run=True)
            self.assertIn("wouldRetry", [event for event, _ in events])
            self.assertEqual((await snapshot(app, thread_id)).status, "notLoaded")
            self.assertEqual(len(self.backend.requests), 1)
            result = await recover(app, thread_id, old)
            self.assertEqual(result["outcome"], "started")
            self.assertEqual((await self.terminal(app, thread_id)).turn["status"], "completed")
            after = (await app.request("thread/goal/get", {"threadId": thread_id}))["goal"]
            for field in ["status", "objective", "tokenBudget", "tokensUsed", "createdAt"]:
                self.assertEqual(after[field], before[field])
            self.assertEqual(len(self.backend.requests), 2)

    async def test_offline_archive_stops_a_known_pending_failure(self):
        events = []
        runner = Runner(delay=100, emit=lambda event, **fields: events.append((event, fields)))
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, _ = await self.create_failed(app)
            await runner.inspect(app, thread_id)
            self.assertIn(thread_id, runner.retries)
        # Another client archives it while the runner is disconnected. On the
        # next connection no archive notification has been seen by the runner.
        async with Native(self.home) as server:
            async with AppServer(f"unix://{server.socket}") as other:
                await other.request("thread/archive", {"threadId": thread_id})
            async with AppServer(f"unix://{server.socket}") as app:
                self.assertNotIn(thread_id, app.archived)
                runner.retries[thread_id].due = 0
                await runner.inspect(app, thread_id)
                self.assertNotIn(thread_id, runner.retries, events)
                self.assertEqual(events[-1][0], "outOfScope")
                self.assertEqual((await snapshot(app, thread_id)).status, "notLoaded")
                self.assertEqual(len(self.backend.requests), 1)

    async def test_idle_legacy_failure_is_discovered_after_cold_load(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, _ = await self.create_failed(app, history_mode="legacy")
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            await app.request("thread/resume", {"threadId": thread_id, "excludeTurns": True})
            current = await snapshot(app, thread_id)
            self.assertEqual(current.status, "idle")
            self.assertTrue(current.capacity_failed)
            runner = Runner(delay=0.001)
            await runner.inspect(app, thread_id)
            runner.retries[thread_id].due = 0
            await runner.inspect(app, thread_id)
            self.assertEqual((await self.terminal(app, thread_id)).turn["status"], "completed")
            self.assertEqual(len(self.backend.requests), 2)

    async def test_read_timeout_after_real_cold_load_does_not_disable_recovery(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, _ = await self.create_failed(app)
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            original = app.request
            loaded = False
            loads = 0

            async def request(method, params):
                nonlocal loaded, loads
                if method == "thread/goal/get" and loaded:
                    raise TimeoutError("injected read timeout after acknowledged load")
                result = await original(method, params)
                if method == "thread/resume":
                    loaded, loads = True, loads + 1
                return result

            app.request = request
            runner = Runner(delay=0.001)
            await runner.inspect(app, thread_id)
            runner.retries[thread_id].due = 0
            await runner.inspect(app, thread_id)
            self.assertFalse(runner.retries[thread_id].blocked)
            self.assertEqual((await snapshot(app, thread_id)).status, "idle")
            app.request = original
            runner.retries[thread_id].due = 0
            await runner.inspect(app, thread_id)
            self.assertEqual((await self.terminal(app, thread_id)).turn["status"], "completed")
            self.assertEqual(loads, 1)
            self.assertEqual(len(self.backend.requests), 2)

    async def test_lost_accepted_turn_reconciles_with_cold_state_after_restart(self):
        runner = Runner(delay=0.001)
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.create_failed(app)
            original = app.request

            async def request(method, params):
                if method == "turn/start":
                    # Fault injection: simulate acceptance followed by loss of
                    # the new turn before it reached durable history. Other RPCs
                    # and the restart use a real isolated Codex server.
                    return {"turn": {"id": "accepted-but-unpersisted"}}
                return await original(method, params)

            app.request = request
            await runner.inspect(app, thread_id)
            runner.retries[thread_id].due = 0
            await runner.inspect(app, thread_id)
            self.assertEqual(runner.retries[thread_id].submitted, "accepted-but-unpersisted")
            await runner.inspect(app, thread_id)
            self.assertEqual(len(self.backend.requests), 1)
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            current = await snapshot(app, thread_id)
            self.assertEqual((current.status, current.turn_id), ("notLoaded", old))
            await runner.inspect(app, thread_id)
            runner.retries[thread_id].due = 0
            await runner.inspect(app, thread_id)
            self.assertEqual((await self.terminal(app, thread_id)).turn["status"], "completed")
            self.assertEqual(len(self.backend.requests), 2)

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

    async def test_supported_legacy_history_is_cached_after_success(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            thread_id, old = await self.create_failed(app, history_mode="legacy")
            await recover(app, thread_id, old)
            await self.terminal(app, thread_id)
            calls = []
            original = app.request

            async def request(method, params):
                calls.append(method)
                return await original(method, params)

            app.request = request
            runner = Runner()
            await runner.inspect(app, thread_id)
            self.assertEqual(calls.count("thread/turns/list"), 1)
            calls.clear()
            await runner.inspect(app, thread_id)
            self.assertEqual(calls, ["thread/read"])
            self.assertEqual(len(self.backend.requests), 2)

    async def test_ephemeral_thread_is_skipped_without_control(self):
        async with Native(self.home) as server, AppServer(f"unix://{server.socket}") as app:
            result = await app.request("thread/start", {"cwd": str(self.home), "ephemeral": True})
            thread_id = result["thread"]["id"]
            self.assertTrue(result["thread"]["ephemeral"])
            calls, events = [], []
            original = app.request

            async def request(method, params):
                calls.append(method)
                return await original(method, params)

            app.request = request
            runner = Runner(emit=lambda event, **fields: events.append((event, fields)))
            await runner.inspect(app, thread_id)
            await runner.inspect(app, thread_id)
            self.assertEqual(calls, ["thread/read"])
            self.assertEqual([event for event, _ in events], ["unsupported"])
            self.assertEqual(len(self.backend.requests), 0)
