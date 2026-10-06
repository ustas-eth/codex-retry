import asyncio
import copy
import time
import unittest

from codex_retry.recovery import CAPACITY_MESSAGE, ControlStopped, Snapshot, recover, snapshot
from codex_retry.rpc import RPCError
from codex_retry.runner import Runner, loaded_threads, recent_threads


def failed(turn_id="old", code="serverOverloaded"):
    return {
        "id": turn_id,
        "status": "failed",
        "error": {
            "message": CAPACITY_MESSAGE,
            "codexErrorInfo": code,
        },
    }


class FakeApp:
    timeout = 0.001

    def __init__(self):
        self.threads = {"one": {"status": "systemError", "turn": failed(), "goal": None}}
        self.calls = []
        self.resume_continues = False
        self.start_error = None
        self.goal_error = None
        self.goal_continues = True
        self.archived = set()
        self.next_turn = {"id": "new", "status": "completed", "error": None}
        self.changed = asyncio.Event()
        self.dirty = set()
        self.reader = asyncio.get_running_loop().create_future()

    async def request(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        if method == "thread/loaded/list":
            return {
                "data": [
                    key
                    for key, value in self.threads.items()
                    if value["status"] != "notLoaded" and key not in self.archived
                ],
                "nextCursor": None,
            }
        if method == "thread/list":
            return {
                "data": [
                    {"id": key, "updatedAt": value.get("updatedAt", time.time())}
                    for key, value in self.threads.items()
                    if key not in self.archived
                ],
                "nextCursor": None,
            }
        thread = self.threads[params["threadId"]]
        if method == "thread/read":
            return {
                "thread": {
                    "status": {"type": thread["status"]},
                    "historyMode": thread.get("historyMode", "paginated"),
                    "updatedAt": thread.get("updatedAt"),
                    "turns": [copy.deepcopy(thread["turn"])] if params.get("includeTurns") else [],
                }
            }
        if method == "thread/turns/list":
            return {"data": [copy.deepcopy(thread["turn"])], "nextCursor": None}
        if method == "thread/goal/get":
            return {"goal": copy.deepcopy(thread["goal"])}
        if method == "thread/goal/set":
            if self.goal_error:
                raise self.goal_error
            thread["goal"]["status"] = params["status"]
            if self.goal_continues:
                thread["turn"] = self.next_turn
                thread["status"] = "idle"
            return {"goal": copy.deepcopy(thread["goal"])}
        if method == "thread/resume":
            thread["status"] = "idle"
            if self.resume_continues:
                thread["turn"] = self.next_turn
            return {"thread": {"id": params["threadId"]}}
        if method == "turn/start":
            if self.start_error:
                raise self.start_error
            thread["turn"] = self.next_turn
            thread["status"] = "idle"
            return {"turn": self.next_turn}
        raise AssertionError(method)


class ClassifierTests(unittest.TestCase):
    def test_only_terminal_capacity_errors(self):
        self.assertTrue(Snapshot("systemError", failed()).capacity_failed)
        self.assertTrue(Snapshot("idle", failed(code=None)).capacity_failed)
        for code in [
            "usageLimitExceeded",
            "rateLimitExceeded",
            "cyberPolicy",
            "other",
            "unauthorized",
        ]:
            self.assertFalse(Snapshot("systemError", failed(code=code)).capacity_failed)
        for turn in [
            None,
            {"id": "x", "status": "completed", "error": failed()["error"]},
            {"id": "x", "status": "failed", "error": None},
            {"id": "x", "status": "failed", "error": {"message": 4}},
        ]:
            self.assertFalse(Snapshot("idle", turn).capacity_failed)


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = FakeApp()

    async def test_empty_turn_has_no_input_or_settings_overrides(self):
        result = await recover(self.app, "one", "old")
        self.assertEqual(result, {"outcome": "started", "turnId": "new"})
        self.assertEqual(self.app.calls[-1], ("turn/start", {"threadId": "one", "input": []}))

    async def test_busy_read_does_not_load_history_or_mutate(self):
        self.app.threads["one"]["status"] = "active"
        self.assertEqual((await recover(self.app, "one", "old"))["outcome"], "changed")
        self.assertEqual([method for method, _ in self.app.calls], ["thread/read"])

    async def test_changed_turn_is_not_retried(self):
        self.app.threads["one"]["turn"] = failed("newer")
        self.assertEqual((await recover(self.app, "one", "old"))["outcome"], "changed")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_inactive_goals_are_not_reactivated(self):
        for status in ["paused", "complete", "budgetLimited", "usageLimited", "unknown"]:
            with self.subTest(status=status):
                objective = {"status": status, "objective": "Fixture", "tokensUsed": 123}
                self.app.threads["one"].update(turn=failed(), goal=objective.copy())
                result = await recover(self.app, "one", "old")
                self.assertEqual(result["outcome"], "started")
                self.assertEqual(self.app.threads["one"]["goal"], objective)
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 5)
        self.assertNotIn("thread/goal/set", [method for method, _ in self.app.calls])

    async def test_capacity_blocked_goal_restores_continuation_without_resetting_budget(self):
        objective = {
            "status": "blocked",
            "objective": "Finish the fixture",
            "tokenBudget": 1000,
            "tokensUsed": 123,
            "timeUsedSeconds": 40,
        }
        self.app.threads["one"]["goal"] = objective.copy()
        self.assertEqual(
            await recover(self.app, "one", "old"),
            {"outcome": "resumed", "turnId": "new", "goalResumed": True},
        )
        self.assertEqual(self.app.threads["one"]["goal"], objective | {"status": "active"})
        self.assertEqual(
            [params for method, params in self.app.calls if method == "thread/goal/set"],
            [{"threadId": "one", "status": "active"}],
        )
        self.assertFalse(any(method == "turn/start" for method, _ in self.app.calls))

    async def test_cold_blocked_goal_loads_then_reactivates_once(self):
        self.app.threads["one"].update(status="notLoaded", goal={"status": "blocked"})
        self.assertTrue((await recover(self.app, "one", "old"))["goalResumed"])
        methods = [method for method, _ in self.app.calls]
        self.assertEqual(methods.count("thread/resume"), 1)
        self.assertEqual(methods.count("thread/goal/set"), 1)
        self.assertNotIn("turn/start", methods)

    async def test_disabled_goal_recovery_leaves_loaded_and_cold_blocked_goals_untouched(self):
        for status in ["systemError", "notLoaded"]:
            with self.subTest(status=status):
                self.app.threads["one"].update(
                    status=status, turn=failed(), goal={"status": "blocked"}
                )
                self.app.calls.clear()
                result = await recover(self.app, "one", "old", resume_blocked_goals=False)
                self.assertEqual(result, {"outcome": "started", "turnId": "new"})
                self.assertEqual(self.app.threads["one"]["goal"], {"status": "blocked"})
                methods = [method for method, _ in self.app.calls]
                self.assertNotIn("thread/goal/set", methods)
                self.assertEqual(methods.count("turn/start"), 1)
                self.assertEqual(methods.count("thread/resume"), int(status == "notLoaded"))

    async def test_disabled_goal_recovery_still_retries_active_and_no_goal_threads(self):
        for objective in [None, {"status": "active"}]:
            with self.subTest(goal=objective):
                self.app.threads["one"].update(turn=failed(), goal=objective)
                result = await recover(self.app, "one", "old", resume_blocked_goals=False)
                self.assertEqual(result, {"outcome": "started", "turnId": "new"})
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 2)
        self.assertNotIn("thread/goal/set", [method for method, _ in self.app.calls])

    async def test_disabled_goal_recovery_still_allows_cold_active_goal_continuation(self):
        self.app.threads["one"].update(status="notLoaded", goal={"status": "active"})
        self.app.resume_continues = True
        result = await recover(self.app, "one", "old", resume_blocked_goals=False)
        self.assertEqual(result, {"outcome": "resumed", "turnId": "new"})
        self.assertNotIn("thread/goal/set", [method for method, _ in self.app.calls])
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_goal_changed_during_preflight_is_not_overridden(self):
        self.app.threads["one"]["goal"] = {"status": "blocked"}
        original = self.app.request
        reads = 0

        async def request(method, params):
            nonlocal reads
            if method == "thread/goal/get":
                reads += 1
                if reads == 2:
                    self.app.threads["one"]["goal"] = {"status": "paused"}
            return await original(method, params)

        self.app.request = request
        self.assertEqual((await recover(self.app, "one", "old"))["outcome"], "changed")
        self.assertNotIn("thread/goal/set", [method for method, _ in self.app.calls])

    async def test_goal_activation_uncertainty_never_adds_a_fallback_turn(self):
        self.app.threads["one"]["goal"] = {"status": "blocked"}
        self.app.goal_error = TimeoutError("lost goal acknowledgement")
        with self.assertRaisesRegex(RuntimeError, "lost goal acknowledgement"):
            await recover(self.app, "one", "old")
        self.app.goal_error = None
        self.app.goal_continues = False
        with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
            await recover(self.app, "one", "old")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_cold_goal_continuation_is_not_started_twice(self):
        self.app.threads["one"].update(status="notLoaded", goal={"status": "active"})
        self.app.resume_continues = True
        self.assertEqual((await recover(self.app, "one", "old"))["outcome"], "resumed")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_unconfirmed_cold_goal_does_not_get_fallback_turn(self):
        self.app.threads["one"].update(status="notLoaded", goal={"status": "active"})
        with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
            await recover(self.app, "one", "old")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_cold_continuation_confirms_active_turn_id(self):
        self.app.threads["one"].update(status="notLoaded", goal={"status": "active"})
        self.app.resume_continues = True
        original = self.app.request

        async def request(method, params):
            result = await original(method, params)
            if method == "thread/resume":
                self.app.threads["one"]["status"] = "active"
            return result

        self.app.request = request
        self.assertEqual(
            await recover(self.app, "one", "old"), {"outcome": "resumed", "turnId": "new"}
        )
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_cold_no_goal_loads_then_starts_once(self):
        self.app.threads["one"]["status"] = "notLoaded"
        self.assertEqual((await recover(self.app, "one", "old"))["outcome"], "started")
        methods = [method for method, _ in self.app.calls]
        self.assertEqual(methods.count("thread/resume"), 1)
        self.assertEqual(methods.count("turn/start"), 1)

    async def test_disabled_goal_feature_is_supported_but_other_goal_error_is_not(self):
        original = self.app.request

        async def request(method, params):
            if method == "thread/goal/get":
                raise RPCError({"code": -32600, "message": "goals feature is disabled"})
            return await original(method, params)

        self.app.request = request
        self.assertEqual((await recover(self.app, "one", "old"))["outcome"], "started")
        self.app.threads["one"]["turn"] = failed()

        async def unsupported(method, params):
            if method == "thread/goal/get":
                raise RPCError({"code": -32600, "message": "unexpected failure"})
            return await original(method, params)

        self.app.request = unsupported
        with self.assertRaisesRegex(RPCError, "unexpected failure"):
            await recover(self.app, "one", "old")

    async def test_small_metadata_page(self):
        await snapshot(self.app, "one")
        self.assertEqual(self.app.calls[-1][1]["itemsView"], "notLoaded")
        self.assertEqual(self.app.calls[-1][1]["limit"], 1)

    async def test_legacy_failure_is_recognized_even_when_idle(self):
        self.app.threads["one"]["historyMode"] = "legacy"
        original = self.app.request

        async def request(method, params):
            if method == "thread/turns/list":
                raise RPCError({"code": -32600, "message": "list_turns is not supported yet"})
            return await original(method, params)

        self.app.request = request
        self.assertTrue((await snapshot(self.app, "one")).capacity_failed)
        self.assertEqual(
            self.app.calls[-1], ("thread/read", {"threadId": "one", "includeTurns": True})
        )
        self.app.calls.clear()
        self.app.threads["one"]["status"] = "idle"
        self.assertTrue((await snapshot(self.app, "one")).capacity_failed)
        self.assertTrue(any(params.get("includeTurns") for _, params in self.app.calls))

    async def test_legacy_cache_reuses_a_read_not_an_assumed_healthy_status(self):
        self.app.threads["one"].update(status="idle", historyMode="legacy", updatedAt=1)
        original = self.app.request

        async def request(method, params):
            if method == "thread/turns/list":
                raise RPCError({"code": -32601, "message": "unsupported"})
            return await original(method, params)

        self.app.request = request
        cache = {}
        self.assertTrue((await snapshot(self.app, "one", legacy_cache=cache)).capacity_failed)
        self.app.calls.clear()
        self.assertTrue((await snapshot(self.app, "one", legacy_cache=cache)).capacity_failed)
        self.assertEqual(
            self.app.calls, [("thread/read", {"threadId": "one", "includeTurns": False})]
        )
        self.app.threads["one"].update(turn=self.app.next_turn, updatedAt=2)
        self.assertFalse((await snapshot(self.app, "one", legacy_cache=cache)).capacity_failed)
        stamp, _, cached = cache["one"]
        cache["one"] = (stamp, 0, cached)
        self.app.calls.clear()
        await snapshot(self.app, "one", legacy_cache=cache)
        self.assertTrue(any(params.get("includeTurns") for _, params in self.app.calls))

    async def test_goal_activation_clamped_by_budget_still_retries_execution(self):
        self.app.threads["one"]["goal"] = {"status": "blocked", "tokensUsed": 50}
        self.app.goal_continues = False
        original = self.app.request

        async def request(method, params):
            result = await original(method, params)
            if method == "thread/goal/set":
                self.app.threads["one"]["goal"]["status"] = "budgetLimited"
                result["goal"]["status"] = "budgetLimited"
            return result

        self.app.request = request
        self.assertEqual((await recover(self.app, "one", "old"))["outcome"], "started")
        self.assertEqual(
            self.app.threads["one"]["goal"], {"status": "budgetLimited", "tokensUsed": 50}
        )

    async def test_final_preflight_leaves_a_newly_active_thread_alone(self):
        original = self.app.request

        async def request(method, params):
            result = await original(method, params)
            if method == "thread/goal/get":
                self.app.threads["one"]["status"] = "active"
            return result

        self.app.request = request
        self.assertEqual((await recover(self.app, "one", "old"))["outcome"], "changed")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_malformed_start_acknowledgement_is_quarantined(self):
        original = self.app.request

        for turn in [None, {}, {"id": ""}, {"id": "old"}]:

            async def request(method, params):
                if method == "turn/start":
                    return {"turn": turn}
                return await original(method, params)

            with self.subTest(turn=turn):
                self.app.request = request
                with self.assertRaises(ControlStopped):
                    await recover(self.app, "one", "old")

    async def test_unrelated_history_error_never_falls_back(self):
        original = self.app.request

        async def request(method, params):
            if method == "thread/turns/list":
                raise RPCError({"code": -32600, "message": "database damaged"})
            return await original(method, params)

        self.app.request = request
        with self.assertRaisesRegex(RPCError, "damaged"):
            await snapshot(self.app, "one")
        self.assertFalse(any(params.get("includeTurns") for _, params in self.app.calls))


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = FakeApp()
        self.events = []
        self.runner = Runner(
            delay=0.001, emit=lambda event, **fields: self.events.append((event, fields))
        )

    async def trigger(self):
        await self.runner.inspect(self.app, "one")
        self.runner.retries["one"].due = 0
        await self.runner.inspect(self.app, "one")

    async def test_server_scan_is_read_only_in_dry_run(self):
        self.app.threads["healthy"] = {"status": "active", "turn": failed(), "goal": None}
        await self.runner.serve(self.app, dry_run=True)
        self.assertEqual([event for event, _ in self.events], ["wouldRetry"])
        self.assertFalse(
            any(method in {"turn/start", "thread/resume"} for method, _ in self.app.calls)
        )

    async def test_one_retry_then_healthy_clears_episode(self):
        await self.trigger()
        await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 1)
        self.assertFalse(self.runner.retries)

    async def test_dry_run_retries_execution_while_preserving_paused_goals(self):
        self.app.threads["one"]["goal"] = {"status": "paused"}
        await self.runner.serve(self.app, dry_run=True)
        self.assertEqual([event for event, _ in self.events], ["wouldRetry"])
        self.assertEqual(self.events[-1][1]["goalStatus"], "paused")
        self.assertIsNone(self.events[-1][1]["goalAction"])
        self.assertFalse(
            any(method in {"turn/start", "thread/resume"} for method, _ in self.app.calls)
        )

    async def test_dry_run_reports_blocked_goals_under_the_selected_policy(self):
        self.app.threads["one"].update(status="notLoaded", goal={"status": "blocked"})
        await self.runner.serve(self.app, dry_run=True)
        self.assertEqual(self.events[-1][0], "wouldRetry")
        self.assertEqual(self.events[-1][1]["goalAction"], "reactivate")
        self.runner.resume_blocked_goals = False
        self.events.clear()
        await self.runner.serve(self.app, dry_run=True)
        self.assertEqual(self.events[-1][0], "wouldRetry")
        self.assertEqual(self.events[-1][1]["goalStatus"], "blocked")
        self.assertIsNone(self.events[-1][1]["goalAction"])
        self.assertFalse(
            any(
                method in {"thread/resume", "turn/start", "thread/goal/set"}
                for method, _ in self.app.calls
            )
        )

    async def test_disabled_goal_recovery_retries_once_without_reactivating_goal(self):
        self.runner = Runner(
            delay=0.001,
            resume_blocked_goals=False,
            emit=lambda event, **fields: self.events.append((event, fields)),
        )
        self.app.threads["one"].update(status="notLoaded", goal={"status": "blocked"})
        await self.trigger()
        await self.runner.inspect(self.app, "one")
        self.assertEqual(self.app.threads["one"]["goal"]["status"], "blocked")
        methods = [method for method, _ in self.app.calls]
        self.assertEqual(methods.count("turn/start"), 1)
        self.assertNotIn("thread/goal/set", methods)

    async def test_repeated_failures_back_off_and_limit_is_per_episode(self):
        self.runner.max_retries = 2
        self.app.next_turn = failed("retry-1")
        await self.trigger()
        await self.runner.inspect(self.app, "one")
        self.assertGreater(self.runner.retries["one"].due, asyncio.get_running_loop().time())
        self.runner.retries["one"].due = 0
        self.app.next_turn = failed("retry-2")
        await self.runner.inspect(self.app, "one")
        await self.runner.inspect(self.app, "one")
        self.runner.retries["one"].due = 0
        await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 2)
        self.assertEqual(self.events[-1][0], "exhausted")

    async def test_unknown_submission_quarantines_only_that_turn(self):
        self.app.start_error = TimeoutError("lost acknowledgement")
        await self.trigger()
        for _ in range(3):
            await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 1)
        self.assertTrue(self.runner.retries["one"].blocked)
        self.app.threads["one"]["turn"] = failed("newer")
        await self.runner.inspect(self.app, "one")
        self.assertFalse(self.runner.retries["one"].blocked)

    async def test_accepted_but_stale_history_does_not_duplicate(self):
        original = self.app.request

        async def request(method, params):
            result = await original(method, params)
            if method == "turn/start":
                self.app.threads["one"]["turn"] = failed()
            return result

        self.app.request = request
        await self.trigger()
        for _ in range(3):
            await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 1)

    async def test_accepted_unpersisted_start_retries_only_after_thread_is_unloaded(self):
        original = self.app.request

        async def request(method, params):
            result = await original(method, params)
            if method == "turn/start":
                self.app.threads["one"]["turn"] = failed()
            return result

        self.app.request = request
        await self.trigger()
        self.assertTrue(self.runner.retries["one"].accepted)
        for _ in range(3):
            await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 1)
        self.app.threads["one"]["status"] = "notLoaded"
        self.app.request = original
        await self.runner.inspect(self.app, "one")
        self.assertIsNone(self.runner.retries["one"].submitted)
        self.assertIn("retryLost", [event for event, _ in self.events])
        self.runner.retries["one"].due = 0
        await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 2)

    async def test_unconfirmed_acknowledged_continuation_can_reconcile_after_unload(self):
        self.app.threads["one"]["goal"] = {"status": "blocked"}
        self.app.goal_continues = False
        await self.trigger()
        pending = self.runner.retries["one"]
        self.assertTrue(pending.blocked)
        self.assertTrue(pending.accepted)
        self.assertEqual(pending.attempts, 1)
        await self.runner.inspect(self.app, "one")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])
        self.app.threads["one"]["status"] = "notLoaded"
        self.app.resume_continues = True
        await self.runner.inspect(self.app, "one")
        self.assertFalse(pending.blocked)
        pending.due = 0
        await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("thread/resume"), 1)
        self.assertEqual(pending.submitted, "new")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_unloaded_retry_reconciliation_does_not_reset_retry_limit(self):
        self.runner.max_retries = 1
        await self.trigger()
        self.app.threads["one"].update(status="notLoaded", turn=failed())
        await self.runner.inspect(self.app, "one")
        self.runner.retries["one"].due = 0
        await self.runner.inspect(self.app, "one")
        self.assertEqual(self.events[-1][0], "exhausted")
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 1)

    async def test_unknown_submission_stays_quarantined_even_after_unload(self):
        self.app.start_error = TimeoutError("lost acknowledgement")
        await self.trigger()
        self.app.threads["one"]["status"] = "notLoaded"
        self.app.start_error = None
        await self.runner.inspect(self.app, "one")
        self.assertTrue(self.runner.retries["one"].blocked)
        self.assertNotIn("thread/resume", [method for method, _ in self.app.calls])

    async def test_unsettled_history_does_not_clear_retry_episode(self):
        await self.trigger()
        self.app.threads["one"]["turn"] = {
            "id": "new",
            "status": "interrupted",
            "completedAt": None,
        }
        await self.runner.inspect(self.app, "one")
        self.assertIn("one", self.runner.retries)

    async def test_reconnect_keeps_uncertain_submission_quarantined(self):
        self.app.start_error = TimeoutError("lost acknowledgement")
        await self.trigger()
        reconnected = FakeApp()
        await self.runner.serve(reconnected, dry_run=True)
        await self.runner.inspect(reconnected, "one")
        self.assertFalse(any(method == "turn/start" for method, _ in reconnected.calls))

    async def test_one_bad_thread_does_not_stop_the_fleet_scan(self):
        self.app.threads["broken"] = {"status": "systemError", "turn": failed(), "goal": None}
        original = self.app.request

        async def request(method, params):
            if params.get("threadId") == "broken":
                raise TimeoutError("history read timed out")
            return await original(method, params)

        self.app.request = request
        await self.runner.serve(self.app, dry_run=True)
        self.assertEqual([event for event, _ in self.events], ["inspectError", "wouldRetry"])

    async def test_unreadable_due_retry_does_not_hot_poll(self):
        await self.runner.inspect(self.app, "one")
        self.runner.retries["one"].due = 0

        async def request(method, params):
            raise RPCError({"code": -32600, "message": "temporarily unreadable"})

        self.app.request = request
        await self.runner.inspect(self.app, "one")
        self.assertGreater(self.runner.retries["one"].due, asyncio.get_running_loop().time())
        self.assertFalse(self.runner.retries["one"].blocked)

    async def test_preflight_read_failure_can_be_retried_without_quarantine(self):
        original = self.app.request

        async def request(method, params):
            if method == "thread/goal/get":
                raise TimeoutError("goal read timed out")
            return await original(method, params)

        self.app.request = request
        await self.trigger()
        self.assertFalse(self.runner.retries["one"].blocked)
        self.assertFalse(any(method == "turn/start" for method, _ in self.app.calls))
        self.app.request = original
        self.runner.retries["one"].due = 0
        await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 1)

    async def test_control_rejection_does_not_loop(self):
        self.app.start_error = RPCError({"code": -32600, "message": "parent-owned thread"})
        await self.trigger()
        await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 1)

    async def test_loaded_goal_pre_activation_read_failure_is_retryable(self):
        self.app.threads["one"]["goal"] = {"status": "blocked"}
        original = self.app.request
        reads = 0

        async def request(method, params):
            nonlocal reads
            if method == "thread/goal/get":
                reads += 1
                if reads == 2:
                    raise TimeoutError("pre-activation read timed out")
            return await original(method, params)

        self.app.request = request
        await self.trigger()
        self.assertFalse(self.runner.retries["one"].blocked)
        self.assertFalse(any(method == "thread/goal/set" for method, _ in self.app.calls))
        self.app.request = original
        self.runner.retries["one"].due = 0
        await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("thread/goal/set"), 1)

    async def test_read_error_after_acknowledged_cold_load_defers_not_quarantines(self):
        for objective in [None, {"status": "paused"}, {"status": "blocked"}]:
            with self.subTest(goal=objective):
                self.app = FakeApp()
                self.runner = Runner(delay=0.001)
                self.app.threads["one"].update(status="notLoaded", goal=copy.deepcopy(objective))
                original = self.app.request
                loaded = False

                async def request(method, params):
                    nonlocal loaded
                    if method == "thread/goal/get" and loaded:
                        raise TimeoutError("read after successful load timed out")
                    result = await original(method, params)
                    if method == "thread/resume":
                        loaded = True
                    return result

                self.app.request = request
                await self.trigger()
                self.assertFalse(self.runner.retries["one"].blocked)
                self.assertNotIn("turn/start", [method for method, _ in self.app.calls])
                self.app.request = original
                self.runner.retries["one"].due = 0
                await self.runner.inspect(self.app, "one")
                methods = [method for method, _ in self.app.calls]
                self.assertEqual(methods.count("thread/resume"), 1)
                self.assertEqual(
                    methods.count("turn/start"), int(objective != {"status": "blocked"})
                )
                self.assertIsNotNone(self.runner.retries["one"].submitted)

    async def test_cache_is_invalidated_by_notifications_and_reconnection(self):
        self.app.threads["one"].update(
            status="idle", historyMode="legacy", updatedAt=1, turn=self.app.next_turn
        )
        original = self.app.request

        async def request(method, params):
            if method == "thread/turns/list":
                raise RPCError({"code": -32601, "message": "unsupported"})
            return await original(method, params)

        self.app.request = request
        task = asyncio.create_task(self.runner.serve(self.app, scan_interval=0.01))
        try:
            for _ in range(100):
                if "one" in self.runner.legacy_cache:
                    break
                await asyncio.sleep(0.005)
            self.assertIn("one", self.runner.legacy_cache)
            self.app.threads["one"]["turn"] = failed()
            self.app.dirty.add("one")
            self.app.changed.set()
            # This shares timestamp 1 with the cached healthy read. A notice
            # must invalidate it even when metadata cannot show the change yet.
            for _ in range(100):
                if any(method == "turn/start" for method, _ in self.app.calls):
                    break
                await asyncio.sleep(0.005)
            self.assertTrue(any(method == "turn/start" for method, _ in self.app.calls))
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.app.threads["one"]["turn"] = self.app.next_turn
        self.runner.retries.clear()
        await self.runner.inspect(self.app, "one")
        self.assertIn("one", self.runner.legacy_cache)
        self.app.calls.clear()
        await self.runner.serve(self.app, dry_run=True)
        self.assertTrue(any(params.get("includeTurns") for _, params in self.app.calls))

    async def test_explicit_scope_loss_drops_pending_retry_without_controls(self):
        await self.runner.inspect(self.app, "one")
        self.runner.retries["one"].due = 0
        original = self.app.request

        async def request(method, params):
            if method == "thread/goal/get":
                raise RPCError({"code": -32600, "message": "thread not found: one"})
            return await original(method, params)

        self.app.request = request
        await self.runner.inspect(self.app, "one")
        self.assertNotIn("one", self.runner.retries)
        self.assertEqual(self.events[-1][0], "outOfScope")
        self.assertNotIn("turn/start", [method for method, _ in self.app.calls])

    async def test_discovery_age_does_not_remove_a_known_pending_retry(self):
        self.runner.delay = 100
        self.app.threads["one"].update(status="notLoaded", updatedAt=time.time())
        task = asyncio.create_task(self.runner.serve(self.app, scan_interval=0.01))
        try:
            for _ in range(100):
                if "one" in self.runner.retries:
                    break
                await asyncio.sleep(0.005)
            pending = self.runner.retries["one"]
            self.app.threads["one"]["updatedAt"] = time.time() - 90000
            # Wait through multiple scans while this is no longer discoverable.
            await asyncio.sleep(0.05)
            self.assertIs(self.runner.retries.get("one"), pending)
            pending.due = 0
            self.app.changed.set()
            for _ in range(100):
                if any(method == "turn/start" for method, _ in self.app.calls):
                    break
                await asyncio.sleep(0.005)
            self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 1)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_newly_loaded_thread_is_discovered(self):
        task = asyncio.create_task(self.runner.serve(self.app, scan_interval=0.01))
        try:
            await asyncio.sleep(0.05)
            self.app.threads["two"] = {
                "status": "systemError",
                "turn": failed("two-old"),
                "goal": None,
            }
            self.app.dirty.add("two")
            self.app.changed.set()
            for _ in range(200):
                starts = [
                    params["threadId"]
                    for method, params in self.app.calls
                    if method == "turn/start"
                ]
                if "two" in starts:
                    break
                await asyncio.sleep(0.01)
            self.assertIn("two", starts)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_unloaded_capacity_failure_is_discovered_after_launch(self):
        self.app.threads["one"].update(status="notLoaded", goal={"status": "blocked"})
        task = asyncio.create_task(self.runner.serve(self.app, scan_interval=0.01))
        try:
            for _ in range(200):
                if any(method == "thread/goal/set" for method, _ in self.app.calls):
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(self.app.threads["one"]["goal"]["status"], "active")
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_unloading_during_backoff_does_not_lose_the_failure(self):
        await self.runner.inspect(self.app, "one")
        self.app.threads["one"]["status"] = "notLoaded"
        self.runner.retries["one"].due = 0
        await self.runner.inspect(self.app, "one")
        self.assertEqual([method for method, _ in self.app.calls].count("thread/resume"), 1)
        self.assertEqual([method for method, _ in self.app.calls].count("turn/start"), 1)

    async def test_archiving_during_backoff_cancels_recovery(self):
        self.runner.delay = 10
        task = asyncio.create_task(self.runner.serve(self.app, scan_interval=0.01))
        try:
            for _ in range(100):
                if "one" in self.runner.retries:
                    break
                await asyncio.sleep(0.005)
            self.app.archived.add("one")
            self.app.threads["one"]["status"] = "notLoaded"
            self.app.changed.set()
            for _ in range(100):
                if "one" not in self.runner.retries:
                    break
                await asyncio.sleep(0.005)
            self.assertNotIn("one", self.runner.retries)
            self.assertFalse(
                any(
                    method in {"thread/resume", "turn/start", "thread/goal/set"}
                    for method, _ in self.app.calls
                )
            )
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_cold_initial_read_failure_is_retried_with_unchanged_metadata(self):
        self.app.threads["one"].update(status="notLoaded", updatedAt=time.time())
        original = self.app.request
        errors = 1

        async def request(method, params):
            nonlocal errors
            if method == "thread/read" and errors:
                errors -= 1
                raise TimeoutError("synthetic first read failure")
            return await original(method, params)

        self.app.request = request
        task = asyncio.create_task(self.runner.serve(self.app, scan_interval=0.01))
        try:
            for _ in range(200):
                if any(method == "turn/start" for method, _ in self.app.calls):
                    break
                await asyncio.sleep(0.005)
            self.assertTrue(any(method == "turn/start" for method, _ in self.app.calls))
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_archived_and_old_saved_threads_are_not_recovered(self):
        self.app.threads["one"]["status"] = "notLoaded"
        self.app.archived.add("one")
        self.app.threads["old"] = {
            "status": "notLoaded",
            "turn": failed(),
            "goal": None,
            "updatedAt": time.time() - 90000,
        }
        await self.runner.serve(self.app, dry_run=True)
        self.assertFalse(any(method == "thread/read" for method, _ in self.app.calls))

    async def test_recent_pagination_stops_at_the_time_boundary(self):
        pages = iter(
            [
                {"data": [{"id": "a", "updatedAt": 30}], "nextCursor": "next"},
                {
                    "data": [{"id": "b", "updatedAt": 20}, {"id": "old", "updatedAt": 1}],
                    "nextCursor": "unused",
                },
            ]
        )

        async def request(method, params):
            self.assertEqual(method, "thread/list")
            self.assertTrue(params["useStateDbOnly"])
            self.assertFalse(params["archived"])
            return next(pages)

        self.app.request = request
        self.assertEqual(await recent_threads(self.app, 10), {"a": 30, "b": 20})

    async def test_loaded_thread_pagination(self):
        pages = iter([{"data": ["a"], "nextCursor": "next"}, {"data": ["b"], "nextCursor": None}])

        async def request(method, params):
            return next(pages)

        self.app.request = request
        self.assertEqual(await loaded_threads(self.app), {"a", "b"})
