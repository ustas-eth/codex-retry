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

The runner covers loaded threads and non-archived saved threads updated in the
last 24 hours. It can be started after a failure, including after the thread
was unloaded or the server restarted. The time window limits discovery;
once a failure is queued, it stays tracked until resolved or archived.
Notifications make it react promptly; a 30-second scan catches missed events
and newly discovered work. The first retry is eligible after 5 seconds, then
backoff grows to a maximum of 60 seconds until capacity returns. Inspection and
recovery add to that delay. Due retries take priority over background scans;
at most two inspections run concurrently, with one slot reserved while a retry
is waiting and only one inspection per thread.
Optional controls:

```sh
codex-retry --delay 10 --max-retries 20
codex-retry --lookback-hours 72
codex-retry --no-resume-blocked-goals
codex-retry --dry-run --json
codex-retry --endpoint unix:///path/to/server.sock
```

`unix://` uses `$CODEX_HOME/app-server-control/app-server-control.sock`, or
`~/.codex/app-server-control/app-server-control.sock` when `CODEX_HOME` is unset.
Explicit `ws://` and `wss://` endpoints are also supported.

## Behavior

The utility reads thread status and the latest turn's error. It matches
`serverOverloaded`, with the exact displayed message as a fallback when the
typed code is absent. Other failures and active turns are left alone. Healthy
saved conversations are inspected, not started; archived conversations are
excluded. Saved discovery uses database metadata rather than scanning log files,
and unchanged unloaded histories are not repeatedly read.

Codex marks an active goal `blocked` when a turn ends in an error. If the latest
turn failed at capacity, the runner restores that goal to `active` so normal
goal-driven work continues, rather than merely starting one extra turn. It
changes only the status: the assignment, budget, and accumulated usage are
retained. Other goal statuses are preserved.

Goal state and execution are separate: a turn can keep running after its goal
is paused, completed, or limited. If that turn fails at capacity, the runner
retries it with empty input without reactivating the goal. The latest terminal
error determines eligibility, not goal status. This restores a chance to
continue from existing context; the model decides what work remains.

Goal recovery is best effort: Codex's goal API does not report why a goal was
blocked. If an agent or controller deliberately blocks a goal and the latest
turn fails at capacity, the runner may reactivate it too. Use
`--no-resume-blocked-goals` to preserve blocked goal status while still retrying
the failed turn. To stop capacity recovery, stop the runner or archive the
thread; pausing a goal alone does not stop retries.

Recovery loads a cold thread when necessary. Goal activation or loading can
start work asynchronously; the runner confirms the new turn and does not send
a duplicate start. With no stopped goal to reactivate, it uses `turn/start`
with empty input when needed, as in `codex-threadctl wake --resume`. It sends no
configuration overrides. Cold loading follows Codex's own resume behavior and
persisted settings; normal account limits and tool permissions still apply.
Parent-owned v2 children may reject direct control. Ephemeral threads expose no
persisted turn history, so the runner skips them and reports `unsupported`
once per connection rather than repeatedly querying an unavailable endpoint.

Read failures defer recovery. Ambiguous control results suspend retries for
that exact failed turn; a later observed capacity failure can be retried.
An acknowledged retry whose new turn did not persist can be retried once the
thread is unloaded. Stale history on a loaded thread is not enough to send a
second start. Rejected control, including native v2 ownership restrictions,
is logged without looping on the same failure.
One local lock prevents duplicate runners on the same endpoint; other
controllers can still race it. An optional retry limit applies per thread's
capacity-failure episode. Retry state is kept in memory; restarting the runner
starts a fresh scan. Server disconnects reconnect automatically.
Ctrl+C stops the runner, not workers or their goals.

Logs contain lifecycle events rather than conversation contents; `--json`
emits one object per line. Recovery results include timing fields:

| Field | Meaning |
| --- | --- |
| `lateMs` | Time past the retry deadline when its inspection began. |
| `recoveryMs` | Total time spent inspecting and recovering in that attempt. |
| `historyMs` / `historyReads` | Time waiting for history requests and their count. |
| `controlMs` | Time waiting for resume, goal activation, or turn-start requests. |

History and control times are parts of `recoveryMs`, not additional delays.
Run the runner in tmux or your service manager to keep it alive. `--dry-run`
works beside an existing runner, exits after one read-only scan, and reports
eligible retries, current goal status, and planned goal reactivation under
the selected policy.

History reads request only the latest turn's metadata. For legacy histories,
Codex can still reconstruct the full rollout before returning that small page.
Recovery reuses its initial inspection and retains a fresh check before changing
the thread. Unchanged idle legacy reads are cached for up to 60 seconds and
invalidated by thread notifications; pending retries always read fresh history.
Idle status alone does not prove success. Older servers without turn pagination
require full-history responses, which can exceed the 16 MiB response limit.
History timeouts and other read failures are logged and defer recovery.

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
