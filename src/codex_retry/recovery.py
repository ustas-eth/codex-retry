"""Only terminal model-capacity failures authorize a retry."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .rpc import RPCError

CAPACITY_MESSAGE = "Selected model is at capacity. Please try a different model."


class ControlStopped(RuntimeError):
    """A control request was rejected or may have been accepted; don't replay it."""

    def __init__(self, message, *, accepted=False):
        super().__init__(message)
        self.accepted = accepted


@dataclass(frozen=True)
class Snapshot:
    status: str
    turn: dict | None

    @property
    def turn_id(self):
        return self.turn["id"] if self.turn else None

    @property
    def capacity_failed(self):
        if not self.turn or self.turn.get("status") != "failed":
            return False
        error = self.turn.get("error")
        if not isinstance(error, dict):
            return False
        code = error.get("codexErrorInfo")
        if code is not None:
            return code == "serverOverloaded"
        message = error.get("message")
        return isinstance(message, str) and message.strip() == CAPACITY_MESSAGE


async def snapshot(app, thread_id, *, active_turn=False, legacy_cache=None):
    result = await app.request("thread/read", {"threadId": thread_id, "includeTurns": False})
    thread = result["thread"]
    status = thread["status"]["type"]
    if status not in {"notLoaded", "idle", "systemError", "active"}:
        raise RuntimeError(f"unknown thread status: {status}")
    now = asyncio.get_running_loop().time()
    stamp = (thread.get("updatedAt"), thread.get("recencyAt"), thread.get("path"))
    cacheable = legacy_cache is not None and status == "idle" and stamp[0] is not None
    if cacheable and thread_id in legacy_cache:
        previous, expires, cached = legacy_cache[thread_id]
        if previous == stamp and now < expires:
            return cached
    if legacy_cache is not None:
        legacy_cache.pop(thread_id, None)
    if status == "active" and not active_turn:
        return Snapshot(status, None)
    legacy = False
    try:
        result = await app.request(
            "thread/turns/list",
            {
                "threadId": thread_id,
                "limit": 1,
                "sortDirection": "desc",
                "itemsView": "notLoaded",
            },
        )
        turns = result["data"]
    except RPCError as exc:
        if exc.payload.get("code") != -32601 and str(exc) != "list_turns is not supported yet":
            raise
        # Idle is a delivery state, not proof that the latest turn succeeded.
        # Legacy history has no bounded turn API; reuse only an actual read.
        full = await app.request("thread/read", {"threadId": thread_id, "includeTurns": True})
        turns = full["thread"]["turns"][-1:]
        legacy = True
    if not isinstance(turns, list):
        raise RuntimeError("invalid turn page")
    turn = turns[0] if turns else None
    if turn is not None and (not isinstance(turn, dict) or not isinstance(turn.get("id"), str)):
        raise RuntimeError("invalid latest turn")
    result = Snapshot(status, turn)
    if legacy and cacheable:
        # Timestamps have second precision. Expire even unchanged metadata so
        # missed notifications cannot conceal a second turn in that same second.
        legacy_cache[thread_id] = (stamp, now + 60, result)
    return result


async def goal(app, thread_id):
    try:
        result = await app.request("thread/goal/get", {"threadId": thread_id})
    except RPCError as exc:
        if exc.payload.get("code") == -32601 or str(exc) == "goals feature is disabled":
            return None
        raise
    value = result["goal"]
    if value is not None and (
        not isinstance(value, dict) or not isinstance(value.get("status"), str)
    ):
        raise RuntimeError("invalid goal status")
    return value


async def continuation(app, thread_id, expected_turn):
    """Goal activation/resume can start a turn asynchronously; don't start twice."""
    deadline = asyncio.get_running_loop().time() + app.timeout
    while True:
        resumed = await snapshot(app, thread_id, active_turn=True)
        if resumed.turn_id is not None and resumed.turn_id != expected_turn:
            return {"outcome": "resumed", "turnId": resumed.turn_id}
        if asyncio.get_running_loop().time() >= deadline:
            raise RuntimeError("goal continuation is unconfirmed; inspect before retrying")
        await asyncio.sleep(0.1)


async def control(app, method, params):
    if params["threadId"] in app.archived:
        raise ControlStopped("thread is archived")
    try:
        return await app.request(method, params)
    except Exception as exc:
        raise ControlStopped(str(exc)) from exc


async def confirmed_continuation(app, thread_id, expected_turn):
    try:
        return await continuation(app, thread_id, expected_turn)
    except Exception as exc:
        # The control was acknowledged, but the resulting turn is not yet known.
        # Observe it rather than adding a fallback turn on the same live thread.
        raise ControlStopped(str(exc), accepted=True) from exc


async def recover(app, thread_id, expected_turn, *, resume_blocked_goals=True):
    """Recheck before mutation. Never retry an uncertain mutation automatically."""
    current = await snapshot(app, thread_id)
    if (
        current.status == "active"
        or current.turn_id != expected_turn
        or not current.capacity_failed
    ):
        return {"outcome": "changed"}
    objective = await goal(app, thread_id)
    if current.status == "notLoaded":
        await control(app, "thread/resume", {"threadId": thread_id, "excludeTurns": True})
        # Cold loading can continue an active goal asynchronously, including a
        # very fast failure. Acknowledged loading of a stopped goal, however,
        # must not turn a subsequent ordinary read failure into a permanent stop.
        if objective and objective["status"] == "active":
            return await confirmed_continuation(app, thread_id, expected_turn)
        after_goal = await goal(app, thread_id)
        if after_goal and after_goal["status"] == "active":
            return await confirmed_continuation(app, thread_id, expected_turn)
        current = await snapshot(app, thread_id)
        if current.status == "active" or current.turn_id != expected_turn:
            return {"outcome": "changed"}
        if objective != after_goal:
            return {"outcome": "changed"}
    # Best effort: the API does not retain why a goal was blocked. Reactivate
    # it after the latest capacity failure unless the caller disables this.
    # An empty turn alone would leave native goal continuation stopped.
    if resume_blocked_goals and objective and objective["status"] == "blocked":
        latest_goal = await goal(app, thread_id)
        latest = await snapshot(app, thread_id)
        if (
            latest_goal != objective
            or latest.status == "active"
            or latest.turn_id != expected_turn
            or not latest.capacity_failed
        ):
            return {"outcome": "changed"}
        updated = await control(app, "thread/goal/set", {"threadId": thread_id, "status": "active"})
        updated_goal = updated.get("goal")
        if not isinstance(updated_goal, dict) or not isinstance(updated_goal.get("status"), str):
            raise ControlStopped("goal activation is unconfirmed; inspect before retrying")
        if updated_goal["status"] == "active":
            result = await confirmed_continuation(app, thread_id, expected_turn)
            return result | {"goalResumed": True}
    # A stopped goal does not stop an already-running turn. Recover the
    # failed execution without reactivating that goal or adding instructions.
    current = await snapshot(app, thread_id)
    if (
        current.status == "active"
        or current.turn_id != expected_turn
        or not current.capacity_failed
    ):
        return {"outcome": "changed"}
    if current.status not in {"idle", "systemError"}:
        raise RuntimeError(f"thread cannot accept an empty turn: {current.status}")
    result = await control(app, "turn/start", {"threadId": thread_id, "input": []})
    turn = result.get("turn")
    turn_id = turn.get("id") if isinstance(turn, dict) else None
    if not isinstance(turn_id, str) or not turn_id or turn_id == expected_turn:
        raise ControlStopped("turn/start acceptance is unconfirmed; inspect before retrying")
    return {"outcome": "started", "turnId": turn_id}
