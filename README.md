# codex-retry

Keep a Codex thread moving when it stops with:

> Selected model is at capacity. Please try a different model.

Run one small process beside your Codex app-server. It automatically retries
capacity-blocked threads, including newly dispatched workers, without adding
user messages or choosing different models. It connects directly to Codex;
ferrumctl is not required.

## Install

Python 3.11+ and a current Codex CLI are required. On Linux or macOS:

```sh
uv tool install git+https://github.com/ustas-eth/codex-retry.git
```

Or install from a checkout with `pip install .`.

## Use

Connect to the same app-server that runs the thread. For example, run the server
in one terminal and attach Codex in another:

```sh
codex app-server --listen unix://
codex --remote unix://
```

Then, in a separate terminal:

```sh
codex-retry
```

The runner covers all loaded threads on that server. Status notifications make
it react promptly; a 30-second scan catches missed events and newly loaded
threads. It retries after 5 seconds, then backs off to a maximum of 60 seconds
until capacity returns. Optional controls:

```sh
codex-retry --delay 10 --max-retries 20
codex-retry --dry-run --json
codex-retry --endpoint unix:///path/to/server.sock
```

`unix://` uses `$CODEX_HOME/app-server-control/app-server-control.sock`, or
`~/.codex/app-server-control/app-server-control.sock` when `CODEX_HOME` is unset.
Explicit `ws://` and `wss://` endpoints are also supported.

## Behavior

The utility reads thread status and the latest turn's error. It matches
`serverOverloaded`, with the exact displayed message as a fallback when the
typed code is absent. Other failures and active turns are left alone. A paused,
completed, usage-limited, or budget-limited goal is left alone. Codex can mark a
goal `blocked` after a capacity error; the runner wakes that thread but leaves
the goal unchanged. It discovers loaded threads rather than reviving old saved
conversations. A thread already seen failing can be loaded again if it becomes
unloaded during recovery.

Recovery follows the same shape as `codex-threadctl wake --resume`: load a cold
thread, account for automatic goal continuation, otherwise request `turn/start`
with empty input. It sends no configuration overrides. Cold loading follows
Codex's own resume behavior and persisted settings; normal account limits and
tool permissions still apply. Parent-owned v2 children may reject direct control.

Ambiguous control results suspend retries for that exact failed turn; a later
observed capacity failure can be retried. Rejected control, including native
v2 ownership restrictions, is logged without looping on the same failure.
One local lock prevents duplicate runners on the same endpoint; other
controllers can still race it. An optional retry limit applies per thread's
capacity-failure episode. Retry state is kept in memory; restarting the runner
starts a fresh scan. Server disconnects reconnect automatically.
Ctrl+C stops the runner, not workers or their goals.

Logs contain lifecycle events rather than conversation contents; `--json`
emits one object per line. Run it in tmux or your service manager to keep it
alive. `--dry-run` exits after one read-only scan. Current history uses a one-turn
metadata read. Legacy threads require a full history read only when failed or
already under recovery; very large histories may exceed the RPC timeout or
16 MiB response limit, in which case the failure is logged and left alone.

The [Codex app-server documentation](https://developers.openai.com/codex/app-server)
describes server setup. Recovery semantics are adapted from
[ferrumctl](https://github.com/ustas-eth/ferrumctl).

## Development

```sh
uv run python -m unittest discover -s tests -v
```

Tests use local synthetic servers. Native Codex smoke tests are opt-in; they use
a temporary Codex home and a local mock model, without account credentials.

```sh
CODEX_RETRY_TEST_BINARY=/path/to/codex uv run python -m unittest discover -s tests -v
```

Native recovery is tested against Codex 0.159.1, including legacy and paginated
history, loaded and cold threads, and goal continuation.
