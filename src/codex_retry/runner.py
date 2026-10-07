"""Capacity recovery for loaded threads and recent non-archived saved work."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass

from .recovery import ControlStopped, HistoryUnavailable, RecoveryTiming, goal, recover, snapshot
from .rpc import RPCError


@dataclass
class Retry:
    turn_id: str
    due: float
    attempts: int = 0
    blocked: bool = False
    submitted: str | None = None
    accepted: bool = False


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
        self.legacy_cache = {}
        self.unsupported = set()
        self.slots = asyncio.Semaphore(2)

    def defer_retry(self, pending):
        if pending and not pending.blocked and pending.submitted is None:
            pending.due = asyncio.get_running_loop().time() + 30

    def unavailable(self, thread_id, exc):
        # Reads can still expose archived history, while goal/control lookup
        # reports it missing. Do not turn that explicit scope loss into a read
        # timeout loop. A later unarchive can be discovered normally.
        if isinstance(exc, RPCError) and str(exc) == f"thread not found: {thread_id}":
            self.retries.pop(thread_id, None)
            self.legacy_cache.pop(thread_id, None)
            self.emit("outOfScope", threadId=thread_id)
            return True
        return False

    async def inspect(self, app, thread_id, *, dry_run=False):
        async with self.slots:
            started = asyncio.get_running_loop().time()
            timing = RecoveryTiming()
            if thread_id in self.unsupported:
                return
            if thread_id in app.archived:
                self.retries.pop(thread_id, None)
                self.legacy_cache.pop(thread_id, None)
                return
            pending = self.retries.get(thread_id)
            late_ms = max(0, started - pending.due) * 1000 if pending else 0
            if pending is not None:
                self.legacy_cache.pop(thread_id, None)
            try:
                current = await snapshot(
                    app,
                    thread_id,
                    legacy_cache=self.legacy_cache if pending is None else None,
                    timing=timing,
                )
            except HistoryUnavailable as exc:
                self.unsupported.add(thread_id)
                self.retries.pop(thread_id, None)
                self.legacy_cache.pop(thread_id, None)
                self.emit("unsupported", threadId=thread_id, message=str(exc))
                return
            except (RuntimeError, TimeoutError, KeyError, TypeError) as exc:
                if self.unavailable(thread_id, exc):
                    return
                self.defer_retry(pending)
                self.emit("inspectError", threadId=thread_id, message=str(exc), **timing.fields())
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
                    if self.unavailable(thread_id, exc):
                        return
                    self.emit("inspectError", threadId=thread_id, message=str(exc))
                    return False
                self.emit(
                    "wouldRetry",
                    threadId=thread_id,
                    turnId=current.turn_id,
                    goalAction="reactivate"
                    if self.resume_blocked_goals and objective and objective["status"] == "blocked"
                    else None,
                    goalStatus=objective["status"] if objective else None,
                )
                return
            now = asyncio.get_running_loop().time()
            if pending is None:
                self.legacy_cache.pop(thread_id, None)
                pending = self.retries[thread_id] = Retry(current.turn_id, now + self.delay)
                self.emit(
                    "scheduled",
                    threadId=thread_id,
                    turnId=current.turn_id,
                    delay=self.delay,
                    **timing.fields(),
                )
            elif current.turn_id != pending.turn_id:
                # A newly observed capacity failure is safe to retry; the old submission is settled.
                pending.turn_id = current.turn_id
                pending.submitted, pending.blocked, pending.accepted = None, False, False
                pending.due = now + min(60, self.delay * 2 ** min(pending.attempts, 10))
                self.emit(
                    "scheduled",
                    threadId=thread_id,
                    turnId=current.turn_id,
                    delay=round(pending.due - now, 2),
                    **timing.fields(),
                )
            elif pending.accepted and current.status == "notLoaded":
                # An acknowledged start/continuation cannot still be running in
                # an unloaded thread. If its turn never persisted, retry the old
                # failure; mere stale history on a loaded thread is not enough.
                self.emit("retryLost", threadId=thread_id, turnId=pending.submitted)
                pending.submitted, pending.blocked, pending.accepted = None, False, False
                pending.due = now + min(60, self.delay * 2 ** min(pending.attempts, 10))
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
                    current=current,
                    timing=timing,
                )
            except ControlStopped as exc:
                pending.blocked = True
                pending.accepted = exc.accepted
                if exc.accepted:
                    pending.attempts += 1
                self.emit(
                    "recoveryStopped", threadId=thread_id, message=str(exc), **timing.fields()
                )
                return
            except (RuntimeError, TimeoutError, KeyError, TypeError) as exc:
                if self.unavailable(thread_id, exc):
                    return
                pending.due = asyncio.get_running_loop().time() + 30
                self.emit(
                    "recoveryDeferred", threadId=thread_id, message=str(exc), **timing.fields()
                )
                return
            self.emit(
                result["outcome"],
                threadId=thread_id,
                **{key: value for key, value in result.items() if key != "outcome"},
                lateMs=round(late_ms, 1),
                recoveryMs=round((asyncio.get_running_loop().time() - started) * 1000, 1),
                **timing.fields(),
            )
            if result["outcome"] in {"started", "resumed"}:
                pending.attempts += 1
                pending.submitted = result["turnId"]
                pending.accepted = True
            elif result["outcome"] == "changed":
                pending.blocked = False
                pending.due = asyncio.get_running_loop().time() + 30

    async def serve(self, app, *, dry_run=False, scan_interval=30):
        self.legacy_cache.clear()
        self.unsupported.clear()
        known, saved_seen, next_scan = set(), {}, 0
        queued, running = set(), {}
        scanned = False
        try:
            while True:
                # Drain notifications before any awaits so notices arriving
                # during discovery are retained for the next iteration.
                notices = set(app.dirty)
                app.dirty.difference_update(notices)
                app.changed.clear()
                for thread_id, task in list(running.items()):
                    if task.done():
                        del running[thread_id]
                        # A notice can arrive before the old read populates
                        # its cache. Its queued follow-up must not reuse that
                        # late cache entry, even within the same second.
                        if thread_id in queued:
                            self.legacy_cache.pop(thread_id, None)
                        if task.result() is False:
                            saved_seen.pop(thread_id, None)
                now = asyncio.get_running_loop().time()
                if not scanned or (not dry_run and now >= next_scan):
                    loaded = await loaded_threads(app)
                    queued |= loaded | set(self.retries)
                    try:
                        saved = await recent_threads(app, time.time() - self.lookback_hours * 3600)
                    except (RuntimeError, TimeoutError, KeyError, TypeError) as exc:
                        self.emit("discoveryError", message=str(exc))
                        saved = saved_seen
                    queued |= {
                        thread_id
                        for thread_id, stamp in saved.items()
                        if saved_seen.get(thread_id) != stamp
                    }
                    saved_seen = saved
                    known = loaded | set(saved) | set(self.retries)
                    for thread_id in set(self.legacy_cache) - known:
                        self.legacy_cache.pop(thread_id, None)
                    self.unsupported.intersection_update(known)
                    queued.intersection_update(known)
                    next_scan = asyncio.get_running_loop().time() + scan_interval
                    scanned = True
                queued |= notices & known
                if notices - known:
                    loaded = await loaded_threads(app)
                    queued |= notices & loaded
                    known |= loaded
                for thread_id in notices:
                    self.legacy_cache.pop(thread_id, None)
                now = asyncio.get_running_loop().time()
                due = {
                    thread_id
                    for thread_id, retry in self.retries.items()
                    if not retry.blocked and retry.submitted is None and retry.due <= now
                }
                if not dry_run:
                    queued |= due
                queued -= app.archived | self.unsupported
                for thread_id in app.archived:
                    if self.retries.pop(thread_id, None) is not None:
                        self.emit("archived", threadId=thread_id)
                if app.reader.done():
                    raise RuntimeError("app-server connection closed")
                # Two reads maximum, one task per thread. Reserve a slot while
                # a retry is waiting; background scans cannot fill it first.
                waiting_retry = not dry_run and any(
                    not retry.blocked and retry.submitted is None for retry in self.retries.values()
                )
                background = sum(thread_id not in due for thread_id in running)
                ordered = sorted(
                    queued - running.keys(),
                    key=lambda thread_id: (
                        thread_id not in due,
                        self.retries[thread_id].due if thread_id in due else 0,
                        thread_id,
                    ),
                )
                for thread_id in ordered:
                    if len(running) == 2:
                        break
                    if thread_id not in due:
                        if waiting_retry and background >= 1:
                            continue
                        background += 1
                    queued.remove(thread_id)
                    task = asyncio.create_task(self.inspect(app, thread_id, dry_run=dry_run))
                    running[thread_id] = task
                    task.add_done_callback(lambda _: app.changed.set())
                if dry_run and not queued and not running:
                    return
                deadlines = [next_scan] if not dry_run else []
                deadlines += [
                    retry.due
                    for thread_id, retry in self.retries.items()
                    if not retry.blocked
                    and retry.submitted is None
                    and thread_id not in running
                    and retry.due > now
                    and not dry_run
                ]
                wait = max(0.01, min(deadlines) - now) if deadlines else None
                try:
                    await asyncio.wait_for(app.changed.wait(), timeout=wait)
                except TimeoutError:
                    pass
        finally:
            for task in running.values():
                task.cancel()
            await asyncio.gather(*running.values(), return_exceptions=True)
