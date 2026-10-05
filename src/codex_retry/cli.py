from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import json
import math
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from websockets.exceptions import WebSocketException

from . import __version__
from .rpc import AppServer, unix_path
from .runner import Runner


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return number


@contextmanager
def controller_lock(endpoint):
    # Prevent two copies of this utility from controlling the same server.
    endpoint = "unix://" + unix_path(endpoint) if endpoint.startswith("unix://") else endpoint
    key = hashlib.sha256(endpoint.encode()).hexdigest()
    root = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "codex-retry"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (root / (key + ".lock")).open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another codex-retry process controls this server") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def parser():
    result = argparse.ArgumentParser(
        description="Automatically recover model-capacity failures across a Codex app-server."
    )
    result.add_argument(
        "--endpoint", default="unix://", help="existing app-server endpoint (default: unix://)"
    )
    result.add_argument(
        "--delay",
        type=positive_float,
        default=5,
        help="first retry delay; backoff is capped at 60s (default: 5)",
    )
    result.add_argument(
        "--max-retries",
        type=int,
        default=0,
        help="per-episode retry limit; 0 keeps trying capacity failures (default: 0)",
    )
    result.add_argument(
        "--timeout", type=positive_float, default=30, help="RPC deadline in seconds (default: 30)"
    )
    result.add_argument(
        "--dry-run",
        action="store_true",
        help="scan loaded threads once without loading or starting any",
    )
    result.add_argument("--json", action="store_true", help="emit JSON lines")
    result.add_argument("--version", action="version", version=f"codex-retry {__version__}")
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    if args.max_retries < 0:
        parser().error("--max-retries must be zero or greater")

    def emit(event, **fields):
        record = {"time": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
        if args.json:
            print(json.dumps(record), flush=True)
        else:
            print(
                f"{record['time']} {event} "
                + " ".join(f"{key}={value}" for key, value in fields.items()),
                flush=True,
            )

    async def execute():
        runner = Runner(delay=args.delay, max_retries=args.max_retries, emit=emit)
        while True:
            try:
                async with AppServer(args.endpoint, args.timeout) as app:
                    emit("connected", endpoint=args.endpoint)
                    await runner.serve(app, dry_run=args.dry_run)
                    return 0
            except (OSError, TimeoutError, RuntimeError, WebSocketException) as exc:
                if args.dry_run:
                    raise
                emit("reconnecting", message=str(exc), delay=5)
                await asyncio.sleep(5)

    try:
        with controller_lock(args.endpoint):
            return asyncio.run(execute())
    except KeyboardInterrupt:
        emit("stopped")
        return 130
    except Exception as exc:
        emit("error", message=str(exc))
        return 1
