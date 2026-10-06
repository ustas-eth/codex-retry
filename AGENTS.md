# Development

This is a slim server-wide runner for terminal model-capacity errors. Discover
loaded and recent non-archived threads, retry only matching failures, and leave
other state alone. Undo the capacity error's consequences: goal status is not
a veto on recovering a failed turn. Preserve paused, completed, and limited
goals while retrying the execution with empty input.
By default, restore a capacity-failed thread's blocked goal
without replacing its objective or resetting its budget/counters. This is
best effort: Codex's goal API does not expose the blocking cause. Keep the
option to preserve blocked goal status consistent with dry-run reporting.
Keep it small: no orchestrator, model fallback, new assignments, prompt
injection, transcript rewriting, or unbounded historical discovery.

Read current app-server schemas before changing RPCs. Match terminal error
metadata, not arbitrary conversation text. Recheck state before mutation, leave
active turns alone, and distinguish request acceptance from completed work.
Cold resume may itself continue a goal; confirm that continuation rather than
adding another start. Ordinary read failures defer recovery, not disable it.
Never replay an uncertain control request. An acknowledged retry can be
reconciled if the thread is unloaded and its new turn did not persist.
The lookback window limits discovery, not an already-known retry's lifetime.
Never broaden permissions or retry policy/usage-limit failures.

Tests use synthetic threads, temporary state, and local mock servers. Real Codex
tests must use an isolated home and local model endpoint, not live threads or
account credentials. Keep private paths, logs, and identifiers out of commits.

Run `uv run python -m unittest discover -s tests -v`, `uvx ruff check .`,
`uvx ruff format --check .`, and `git diff --check`.
Use conventional commit headers that explain the purpose of the change.
