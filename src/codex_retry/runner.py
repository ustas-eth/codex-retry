"""Capacity recovery for loaded threads and recent non-archived saved work."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass

from .recovery import ControlStopped, goal, recover, retryable_goal, snapshot


@dataclass
class Retry:
    turn_id: str
    due: float
    attempts: int = 0
    blocked: bool = False
    submitted: str | None = None
    inactive_goal: str | None = None


async def loaded_threads(app):
    found, cursor, seen = set(), None, set()
    while True:
        result = await app.request("thread/loaded/list", {"cursor": cursor} if cursor else {})
        data = result["data"]
        if not isinstance(data, list) or not all(isinstance(item, str) for item in data):
            raise RuntimeError("invalid loaded thread list")
        found.update(data)
        cursor = result.get("nextCursor")
        if cursor is None:
            return found
        if not isinstance(cursor, str) or cursor in seen:
            raise RuntimeError("invalid loaded thread cursor")
        seen.add(cursor)


async def recent_threads(app, since):
    """Metadata-only discovery includes unloaded, non-archived work."""
    found, cursor, seen = {}, None, set()
    while True:
        params = {
            "limit": 100,
            "sortKey": "updated_at",
            "sortDirection": "desc",
            "archived": False,
            "useStateDbOnly": True,
            "sourceKinds": ["cli", "vscode", "exec", "appServer", "subAgent", "unknown"],
        }
        if cursor:
            params["cursor"] = cursor
        result = await app.request("thread/list", params)
        data = result["data"]
        if not isinstance(data, list):
            raise RuntimeError("invalid saved thread list")
        for thread in data:
            if not isinstance(thread, dict) or not isinstance(thread.get("id"), str):
                raise RuntimeError("invalid saved thread identity")
            updated = thread.get("updatedAt")
            if (
                not isinstance(updated, (int, float))
                or isinstance(updated, bool)
                or not math.isfinite(updated)
            ):
                raise RuntimeError("invalid saved thread timestamp")
            if updated < since:
                return found
            found[thread["id"]] = updated
        cursor = result.get("nextCursor")
        if cursor is None:
            return found
        if not isinstance(cursor, str) or cursor in seen:
            raise RuntimeError("invalid saved thread cursor")
        seen.add(cursor)


class Runner:
    def __init__(
        self,
        *,
        delay=5,
        max_retries=0,
        lookback_hours=24,
        resume_blocked_goals=True,
        emit=lambda *args, **kwargs: None,
    ):
        self.delay, self.max_retries, self.emit = delay, max_retries, emit
        self.lookback_hours = lookback_hours
        self.resume_blocked_goals = resume_blocked_goals
        self.retries = {}
        self.slots = asyncio.Semaphore(2)

    def defer_retry(self, pending):
        if pending and not pending.blocked and pending.submitted is None:
            pending.due = asyncio.get_running_loop().time() + 30

    async def inspect(self, app, thread_id, *, dry_run=False):
        async with self.slots:
            pending = self.retries.get(thread_id)
            try:
                current = await snapshot(app, thread_id, inspect_idle=thread_id in self.retries)
            except (RuntimeError, TimeoutError, KeyError, TypeError) as exc:
                self.defer_retry(pending)
                self.emit("inspectError", threadId=thread_id, message=str(exc))
                return False
            if current.status == "active":
                self.defer_retry(pending)
                return
            if not current.capacity_failed:
                if (
                    current.turn
                    and current.turn.get("status") in {"inProgress", "interrupted"}
                    and not current.turn.get("completedAt")
                ):
                    self.defer_retry(pending)
                    return
                if pending is not None:
                    self.retries.pop(thread_id, None)
                    self.emit("cleared", threadId=thread_id, turnId=current.turn_id)
                return
            if dry_run:
                try:
                    objective = await goal(app, thread_id)
                except (RuntimeError, TimeoutError, KeyError, TypeError) as exc:
                    self.emit("inspectError", threadId=thread_id, message=str(exc))
                    return False
                if not retryable_goal(objective, self.resume_blocked_goals):
                    self.emit("inactiveGoal", threadId=thread_id, goalStatus=objective["status"])
                    return
                self.emit(
                    "wouldRetry",
                    threadId=thread_id,
                    turnId=current.turn_id,
                    goalAction="reactivate"
                    if objective and objective["status"] == "blocked"
                    else None,
                )
                return
            now = asyncio.get_running_loop().time()
            if pending is None:
                pending = self.retries[thread_id] = Retry(current.turn_id, now + self.delay)
                self.emit("scheduled", threadId=thread_id, turnId=current.turn_id, delay=self.delay)
            elif current.turn_id != pending.turn_id:
                # A newly observed capacity failure is safe to retry; the old submission is settled.
                pending.turn_id, pending.submitted, pending.blocked = current.turn_id, None, False
                pending.inactive_goal = None
                pending.due = now + min(60, self.delay * 2 ** min(pending.attempts, 10))
                self.emit(
                    "scheduled",
                    threadId=thread_id,
                    turnId=current.turn_id,
                    delay=round(pending.due - now, 2),
                )
            if pending.blocked or pending.submitted is not None or now < pending.due:
                return
            if self.max_retries and pending.attempts >= self.max_retries:
                pending.blocked = True
                self.emit("exhausted", threadId=thread_id, retries=pending.attempts)
                return
            # Any uncertain mutation quarantines this exact failed turn. A different
            # observed turn can establish a fresh failure without replaying the old request.
            try:
                result = await recover(
                    app,
                    thread_id,
                    pending.turn_id,
                    resume_blocked_goals=self.resume_blocked_goals,
                )
            except ControlStopped as exc:
                pending.blocked = True
                self.emit("recoveryStopped", threadId=thread_id, message=str(exc))
                return
            except (RuntimeError, TimeoutError, KeyError, TypeError) as exc:
                pending.due = now + 30
                self.emit("recoveryDeferred", threadId=thread_id, message=str(exc))
                return
            if result["outcome"] != "inactiveGoal" or pending.inactive_goal != result["goalStatus"]:
                self.emit(
                    result["outcome"],
                    threadId=thread_id,
                    **{key: value for key, value in result.items() if key != "outcome"},
                )
            pending.inactive_goal = result.get("goalStatus")
            if result["outcome"] in {"started", "resumed"}:
                pending.attempts += 1
                pending.submitted = result["turnId"]
            elif result["outcome"] in {"changed", "inactiveGoal"}:
                pending.blocked = False
                pending.due = now + 30

    async def serve(self, app, *, dry_run=False, scan_interval=30):
        known, saved_seen, next_scan = set(), {}, 0
        while True:
            now = asyncio.get_running_loop().time()
            if now >= next_scan:
                loaded = await loaded_threads(app)
                targets = loaded | set(self.retries)
                try:
                    saved = await recent_threads(app, time.time() - self.lookback_hours * 3600)
                except (RuntimeError, TimeoutError, KeyError, TypeError) as exc:
                    self.emit("discoveryError", message=str(exc))
                    saved = saved_seen
                targets |= {
                    thread_id
                    for thread_id, stamp in saved.items()
                    if saved_seen.get(thread_id) != stamp
                }
                removed = set(saved_seen) - set(saved) - loaded
                targets -= removed
                for thread_id in removed:
                    if self.retries.pop(thread_id, None) is not None:
                        self.emit("outOfScope", threadId=thread_id)
                saved_seen = saved
                known = loaded | set(saved) | set(self.retries)
                next_scan = now + scan_interval
            else:
                targets = app.dirty & known
                if app.dirty - known:
                    loaded = await loaded_threads(app)
                    targets |= app.dirty & loaded
                    known |= loaded
            targets |= {
                thread_id
                for thread_id, retry in self.retries.items()
                if not retry.blocked and retry.submitted is None and retry.due <= now
            }
            app.dirty.clear()
            app.changed.clear()
            targets -= app.archived
            for thread_id in app.archived:
                if self.retries.pop(thread_id, None) is not None:
                    self.emit("archived", threadId=thread_id)
            if app.reader.done():
                raise RuntimeError("app-server connection closed")
            ordered = sorted(targets)
            results = await asyncio.gather(
                *(self.inspect(app, thread_id, dry_run=dry_run) for thread_id in ordered)
            )
            for thread_id, inspected in zip(ordered, results):
                if inspected is False:
                    saved_seen.pop(thread_id, None)
            if dry_run:
                return
            deadlines = [next_scan] + [
                retry.due
                for retry in self.retries.values()
                if not retry.blocked and retry.submitted is None
            ]
            wait = max(0.05, min(deadlines) - asyncio.get_running_loop().time())
            try:
                await asyncio.wait_for(app.changed.wait(), timeout=wait)
            except TimeoutError:
                pass
