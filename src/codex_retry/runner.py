"""Server-wide capacity recovery, without scanning saved conversations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .recovery import ControlStopped, goal, recover, snapshot


@dataclass
class Retry:
    turn_id: str
    due: float
    attempts: int = 0
    blocked: bool = False
    submitted: str | None = None


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


class Runner:
    def __init__(self, *, delay=5, max_retries=0, emit=lambda *args, **kwargs: None):
        self.delay, self.max_retries, self.emit = delay, max_retries, emit
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
                return
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
                    return
                if objective and objective["status"] not in {"active", "blocked"}:
                    self.emit("inactiveGoal", threadId=thread_id, goalStatus=objective["status"])
                    return
                self.emit("wouldRetry", threadId=thread_id, turnId=current.turn_id)
                return
            now = asyncio.get_running_loop().time()
            if pending is None:
                pending = self.retries[thread_id] = Retry(current.turn_id, now + self.delay)
                self.emit("scheduled", threadId=thread_id, turnId=current.turn_id, delay=self.delay)
            elif current.turn_id != pending.turn_id:
                # A newly observed capacity failure is safe to retry; the old submission is settled.
                pending.turn_id, pending.submitted, pending.blocked = current.turn_id, None, False
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
                result = await recover(app, thread_id, pending.turn_id)
            except ControlStopped as exc:
                pending.blocked = True
                self.emit("recoveryStopped", threadId=thread_id, message=str(exc))
                return
            except (RuntimeError, TimeoutError, KeyError, TypeError) as exc:
                pending.due = now + 30
                self.emit("recoveryDeferred", threadId=thread_id, message=str(exc))
                return
            self.emit(
                result["outcome"],
                threadId=thread_id,
                **{key: value for key, value in result.items() if key != "outcome"},
            )
            if result["outcome"] in {"started", "resumed"}:
                pending.attempts += 1
                pending.submitted = result["turnId"]
            elif result["outcome"] in {"changed", "inactiveGoal"}:
                pending.blocked = False
                pending.due = now + 30

    async def serve(self, app, *, dry_run=False, scan_interval=30):
        known, next_scan = set(), 0
        while True:
            now = asyncio.get_running_loop().time()
            if now >= next_scan:
                loaded = await loaded_threads(app)
                targets = loaded | set(self.retries)
                known = loaded | set(self.retries)
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
            if app.reader.done():
                raise RuntimeError("app-server connection closed")
            await asyncio.gather(
                *(self.inspect(app, thread_id, dry_run=dry_run) for thread_id in sorted(targets))
            )
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
