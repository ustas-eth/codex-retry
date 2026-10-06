import asyncio
import contextlib
import io
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from websockets.asyncio.server import serve

from codex_retry.cli import controller_lock, main, parser
from codex_retry.rpc import AppServer, RPCError, unix_path
from codex_retry.runner import Runner


class RPCTests(unittest.IsolatedAsyncioTestCase):
    async def test_rpc_handshake_notifications_concurrent_requests_and_error(self):
        async def handler(ws):
            async for line in ws:
                message = json.loads(line)
                if "id" not in message:
                    continue
                await ws.send(
                    json.dumps(
                        {"method": "thread/status/changed", "params": {"threadId": "example"}}
                    )
                )
                if message["method"] == "reject":
                    await ws.send(
                        json.dumps(
                            {"id": message["id"], "error": {"code": -32600, "message": "rejected"}}
                        )
                    )
                else:
                    await ws.send(
                        json.dumps({"id": message["id"], "result": {"method": message["method"]}})
                    )

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            async with AppServer(f"ws://127.0.0.1:{port}") as app:
                values = await asyncio.gather(app.request("a", {}), app.request("b", {}))
                self.assertEqual(values, [{"method": "a"}, {"method": "b"}])
                self.assertIn("example", app.dirty)
                with self.assertRaisesRegex(RPCError, "rejected"):
                    await app.request("reject", {})

    async def test_timeout_and_connection_close_do_not_hang(self):
        async def handler(ws):
            async for line in ws:
                message = json.loads(line)
                if message.get("method") == "initialize":
                    await ws.send(json.dumps({"id": message["id"], "result": {}}))
                elif message.get("method") == "close":
                    await ws.close()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            async with AppServer(f"ws://127.0.0.1:{port}", timeout=0.1) as app:
                with self.assertRaises(TimeoutError):
                    await app.request("hang", {})
                self.assertEqual(app.pending, {})
                with self.assertRaisesRegex(RuntimeError, "closed"):
                    await app.request("close", {})


class CLITests(unittest.TestCase):
    def test_dry_run_does_not_take_the_running_controllers_lock(self):
        class EmptyServer:
            async def __aenter__(self):
                self.reader = asyncio.get_running_loop().create_future()
                self.dirty, self.archived = set(), set()
                self.changed = asyncio.Event()
                return self

            async def __aexit__(self, *_):
                pass

            async def request(self, method, params):
                if method not in {"thread/list", "thread/loaded/list"}:
                    raise AssertionError(method)
                return {"data": [], "nextCursor": None}

        for options, resume in [([], True), (["--no-resume-blocked-goals"], False)]:
            with (
                self.subTest(resume_blocked_goals=resume),
                patch("codex_retry.cli.controller_lock") as lock,
                patch("codex_retry.cli.AppServer", return_value=EmptyServer()),
                patch("codex_retry.cli.Runner", wraps=Runner) as runner,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(main(["--dry-run", *options]), 0)
                lock.assert_not_called()
                self.assertEqual(runner.call_args.kwargs["resume_blocked_goals"], resume)

    def test_server_wide_default_and_validation(self):
        self.assertEqual(parser().parse_args([]).delay, 5)
        self.assertEqual(parser().parse_args([]).lookback_hours, 24)
        self.assertTrue(parser().parse_args([]).resume_blocked_goals)
        self.assertFalse(parser().parse_args(["--no-resume-blocked-goals"]).resume_blocked_goals)
        for value in ["0", "-1", "nan", "inf"]:
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser().parse_args(["--delay", value])
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser().parse_args(["--lookback-hours", value])

    def test_lock_is_server_scoped_and_releases(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {"XDG_CACHE_HOME": directory}),
        ):
            with controller_lock("ws://localhost:1"):
                with self.assertRaisesRegex(RuntimeError, "another"):
                    with controller_lock("ws://localhost:1"):
                        self.fail("lock was not enforced")
                with controller_lock("ws://localhost:2"):
                    pass
            with controller_lock("ws://localhost:1"):
                pass

    def test_default_socket_obeys_codex_home(self):
        with patch.dict(os.environ, {"CODEX_HOME": "/example"}):
            self.assertEqual(
                unix_path("unix://"), "/example/app-server-control/app-server-control.sock"
            )
