"""Only terminal model-capacity failures authorize a retry."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .rpc import RPCError

CAPACITY_MESSAGE = "Selected model is at capacity. Please try a different model."


class ControlStopped(RuntimeError):
    """A control request was rejected or may have been accepted; don't replay it."""


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


async def snapshot(app, thread_id, *, active_turn=False, inspect_idle=False):
    result = await app.request("thread/read", {"threadId": thread_id, "includeTurns": False})
    thread = result["thread"]
    status = thread["status"]["type"]
    if status not in {"notLoaded", "idle", "systemError", "active"}:
        raise RuntimeError(f"unknown thread status: {status}")
    if status == "active" and not active_turn:
        return Snapshot(status, None)
    if thread.get("historyMode") == "legacy" and status == "idle" and not inspect_idle:
        return Snapshot(status, None)
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
        # Legacy history has no bounded turn API. Only hydrate it for a failed
        # thread or a recovery already in progress, never routine healthy scans.
        if status == "idle" and not inspect_idle:
            return Snapshot(status, None)
        full = await app.request("thread/read", {"threadId": thread_id, "includeTurns": True})
        turns = full["thread"]["turns"][-1:]
    if not isinstance(turns, list):
        raise RuntimeError("invalid turn page")
    turn = turns[0] if turns else None
    if turn is not None and (not isinstance(turn, dict) or not isinstance(turn.get("id"), str)):
        raise RuntimeError("invalid latest turn")
    return Snapshot(status, turn)


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


async def recover(app, thread_id, expected_turn):
    """Recheck before mutation. Never retry an uncertain mutation automatically."""
    current = await snapshot(app, thread_id, inspect_idle=True)
    if (
        current.status == "active"
        or current.turn_id != expected_turn
        or not current.capacity_failed
    ):
        return {"outcome": "changed"}
    objective = await goal(app, thread_id)
    if objective and objective["status"] not in {"active", "blocked"}:
        return {"outcome": "inactiveGoal", "goalStatus": objective["status"]}
    try:
        if current.status == "notLoaded":
            await app.request("thread/resume", {"threadId": thread_id, "excludeTurns": True})
            # Cold loading can continue a goal asynchronously, including a very fast failure.
            after_goal = await goal(app, thread_id)
            if (objective and objective["status"] == "active") or (
                after_goal and after_goal["status"] == "active"
            ):
                deadline = asyncio.get_running_loop().time() + app.timeout
                while True:
                    resumed = await snapshot(app, thread_id, active_turn=True, inspect_idle=True)
                    if resumed.turn_id is not None and resumed.turn_id != expected_turn:
                        return {"outcome": "resumed", "turnId": resumed.turn_id}
                    if asyncio.get_running_loop().time() >= deadline:
                        raise RuntimeError(
                            "goal continuation is unconfirmed after loading; inspect before retrying"
                        )
                    await asyncio.sleep(0.1)
            if after_goal and after_goal["status"] not in {"active", "blocked"}:
                return {"outcome": "inactiveGoal", "goalStatus": after_goal["status"]}
            current = await snapshot(app, thread_id, inspect_idle=True)
            if current.status == "active" or current.turn_id != expected_turn:
                return {"outcome": "changed"}
        if current.status not in {"idle", "systemError"}:
            raise RuntimeError(f"thread cannot accept an empty turn: {current.status}")
        result = await app.request("turn/start", {"threadId": thread_id, "input": []})
        turn_id = result.get("turn", {}).get("id")
        if not isinstance(turn_id, str) or not turn_id:
            raise RuntimeError("turn/start acceptance is unconfirmed; inspect before retrying")
        return {"outcome": "started", "turnId": turn_id}
    except Exception as exc:
        raise ControlStopped(str(exc)) from exc
