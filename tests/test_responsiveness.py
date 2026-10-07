"""Slow reads, concurrent discovery, and cancellation must not weaken recovery."""

import asyncio
import contextlib
import unittest

from test_recovery import FakeApp, failed

from codex_retry.recovery import HistoryUnavailable, snapshot
from codex_retry.runner import Retry, Runner


class ResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = FakeApp()
        self.events = []
        self.runner = Runner(
            delay=0.02, emit=lambda event, **fields: self.events.append((event, fields))
        )

    async def stop(self, task):
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def test_due_recovery_reuses_entry_read_but_keeps_final_preflight(self):
        await self.runner.inspect(self.app, "one")
        self.runner.retries["one"].due = 0
        self.app.calls.clear()
        await self.runner.inspect(self.app, "one")
        methods = [method for method, _ in self.app.calls]
        self.assertEqual(methods.count("thread/turns/list"), 2)
        self.assertEqual(methods.count("turn/start"), 1)
        event, fields = self.events[-1]
        self.assertEqual(event, "started")
        self.assertEqual(fields["historyReads"], 2)
        for name in ["lateMs", "recoveryMs", "historyMs", "controlMs"]:
            self.assertGreaterEqual(fields[name], 0)

    async def test_new_terminal_turn_during_goal_read_prevents_stale_retry(self):
        await self.runner.inspect(self.app, "one")
        self.runner.retries["one"].due = 0
        original = self.app.request

        async def request(method, params):
            result = await original(method, params)
            if method == "thread/goal/get":
                self.app.threads["one"]["turn"] = self.app.next_turn
            return result

        self.app.request = request
        await self.runner.inspect(self.app, "one")
        self.assertEqual(self.events[-1][0], "changed")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_supported_legacy_pagination_is_cached_only_outside_recovery(self):
        self.app.threads["one"].update(
            status="idle", historyMode="legacy", updatedAt=1, turn=self.app.next_turn
        )
        await self.runner.inspect(self.app, "one")
        self.assertIn("one", self.runner.legacy_cache)
        self.app.calls.clear()
        await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls], ["thread/read"])
        # Same-second metadata cannot authorize using a cached healthy turn
        # after a capacity failure is already known.
        self.app.threads["one"]["turn"] = failed()
        self.runner.retries["one"] = Retry("old", 0)
        self.app.calls.clear()
        await self.runner.inspect(self.app, "one")
        self.assertEqual(self.events[-1][0], "started")
        self.assertEqual([method for method, _ in self.app.calls].count("thread/turns/list"), 2)
        self.assertNotIn("one", self.runner.legacy_cache)

    async def test_slow_background_reads_cannot_fill_reserved_retry_slot(self):
        self.app.threads = {
            "a-capacity": {"status": "systemError", "turn": failed(), "goal": None},
            **{
                name: {"status": "idle", "turn": self.app.next_turn, "goal": None}
                for name in ["b-slow", "c-slow", "d-slow"]
            },
        }
        release = asyncio.Event()
        started = asyncio.Event()
        original = self.app.request
        active, maximum, per_thread = 0, 0, {}

        async def request(method, params):
            nonlocal active, maximum
            if method != "thread/turns/list":
                return await original(method, params)
            thread_id = params["threadId"]
            active += 1
            maximum = max(maximum, active)
            per_thread[thread_id] = per_thread.get(thread_id, 0) + 1
            self.assertEqual(per_thread[thread_id], 1)
            try:
                if thread_id != "a-capacity":
                    await release.wait()
                return await original(method, params)
            finally:
                per_thread[thread_id] -= 1
                active -= 1

        def emit(event, **fields):
            self.events.append((event, fields))
            if event == "started":
                started.set()

        self.app.request = request
        self.runner.emit = emit
        task = asyncio.create_task(self.runner.serve(self.app, scan_interval=10))
        try:
            # Background reads never finish. The old gather-based loop cannot
            # reach the deadline; the retry must start without releasing them.
            await asyncio.wait_for(started.wait(), 1)
            self.assertFalse(release.is_set())
            self.assertLessEqual(maximum, 2)
            self.assertEqual(
                [params["threadId"] for method, params in self.app.calls if method == "turn/start"],
                ["a-capacity"],
            )
            release.set()
            for _ in range(100):
                if any(
                    method == "thread/turns/list" and params["threadId"] == "d-slow"
                    for method, params in self.app.calls
                ):
                    break
                await asyncio.sleep(0.005)
            else:
                self.fail("background queue was starved after retry")
        finally:
            await self.stop(task)
        self.assertEqual(active, 0)

    async def test_notice_arriving_during_discovery_is_not_cleared(self):
        self.app.threads["one"]["turn"] = self.app.next_turn
        original = self.app.request
        started = asyncio.Event()
        first = True

        async def request(method, params):
            nonlocal first
            if method == "thread/list" and first:
                first = False
                result = await original(method, params)
                self.app.threads["two"] = {
                    "status": "systemError",
                    "turn": failed("two-old"),
                    "goal": None,
                }
                self.app.dirty.add("two")
                self.app.changed.set()
                await asyncio.sleep(0)
                return result
            if method == "turn/start" and params["threadId"] == "two":
                started.set()
            return await original(method, params)

        self.app.request = request
        task = asyncio.create_task(self.runner.serve(self.app, scan_interval=10))
        try:
            await asyncio.wait_for(started.wait(), 1)
        finally:
            await self.stop(task)

    async def test_notice_during_legacy_read_cannot_repopulate_a_stale_cache(self):
        self.app.threads["one"].update(
            status="idle", historyMode="legacy", updatedAt=1, turn=self.app.next_turn
        )
        original = self.app.request
        reading, release, started = asyncio.Event(), asyncio.Event(), asyncio.Event()
        first = True

        async def request(method, params):
            nonlocal first
            result = await original(method, params)
            if method == "thread/turns/list" and first:
                first = False
                reading.set()
                await release.wait()
            if method == "turn/start":
                started.set()
            return result

        self.app.request = request
        task = asyncio.create_task(self.runner.serve(self.app, scan_interval=10))
        try:
            await asyncio.wait_for(reading.wait(), 1)
            self.app.threads["one"]["turn"] = failed()
            self.app.dirty.add("one")
            self.app.changed.set()
            # Let the dispatcher consume the notice before the old request
            # returns. That return must not undo the invalidation.
            for _ in range(100):
                if "one" not in self.app.dirty:
                    break
                await asyncio.sleep(0.005)
            self.assertNotIn("one", self.app.dirty)
            release.set()
            await asyncio.wait_for(started.wait(), 1)
        finally:
            await self.stop(task)

    async def test_dry_run_with_due_retry_finishes_without_control(self):
        self.runner.retries["one"] = Retry("old", 0)
        await asyncio.wait_for(self.runner.serve(self.app, dry_run=True), 1)
        self.assertEqual([event for event, _ in self.events], ["wouldRetry"])
        self.assertFalse(
            any(
                method in {"turn/start", "thread/goal/set", "thread/resume"}
                for method, _ in self.app.calls
            )
        )

    async def test_ephemeral_threads_are_reported_once_without_history_requests(self):
        original = self.app.request

        async def request(method, params):
            result = await original(method, params)
            if method == "thread/read":
                result["thread"]["ephemeral"] = True
            return result

        self.app.request = request
        with self.assertRaises(HistoryUnavailable):
            await snapshot(self.app, "one")
        self.app.calls.clear()
        await self.runner.inspect(self.app, "one")
        await self.runner.inspect(self.app, "one")
        self.assertEqual([event for event, _ in self.events], ["unsupported"])
        self.assertEqual([method for method, _ in self.app.calls], ["thread/read"])
        # A new connection re-evaluates capabilities rather than carrying an
        # exclusion indefinitely across servers.
        await self.runner.serve(self.app, dry_run=True)
        self.assertEqual([event for event, _ in self.events], ["unsupported", "unsupported"])

    async def test_cancelling_uncertain_start_quarantines_it_before_reconnect(self):
        submitted = asyncio.Event()
        original = self.app.request

        async def request(method, params):
            if method == "turn/start":
                submitted.set()
                await asyncio.Event().wait()
            return await original(method, params)

        self.app.request = request
        task = asyncio.create_task(self.runner.serve(self.app))
        await asyncio.wait_for(submitted.wait(), 1)
        await self.stop(task)
        self.assertTrue(self.runner.retries["one"].blocked)
        self.assertFalse(self.runner.retries["one"].accepted)
        self.app.request = original
        await self.runner.inspect(self.app, "one")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_cancelling_acknowledged_goal_continuation_preserves_acceptance(self):
        self.app.threads["one"]["goal"] = {"status": "blocked"}
        self.app.goal_continues = False
        activated = asyncio.Event()
        original = self.app.request

        async def request(method, params):
            result = await original(method, params)
            if method == "thread/goal/set":
                activated.set()
            if method == "thread/read" and activated.is_set():
                await asyncio.Event().wait()
            return result

        self.app.request = request
        task = asyncio.create_task(self.runner.serve(self.app))
        await asyncio.wait_for(activated.wait(), 1)
        await self.stop(task)
        pending = self.runner.retries["one"]
        self.assertTrue(pending.blocked)
        self.assertTrue(pending.accepted)
        self.assertEqual(pending.attempts, 1)
