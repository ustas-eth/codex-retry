# Development

This is a slim server-wide runner for terminal model-capacity errors. Discover
loaded threads, retry only matching failures, and leave other state alone.
Keep it small: no orchestrator, model fallback, goal editing, prompt injection,
transcript rewriting, or sweeping through saved historical conversations.

Read current app-server schemas before changing RPCs. Match terminal error
metadata, not arbitrary conversation text. Recheck state before mutation, leave
active turns alone, and distinguish request acceptance from completed work.
Cold resume may itself continue a goal; uncertain control must stop, not retry.
Never broaden permissions or retry policy/usage-limit failures.

Tests use synthetic threads, temporary state, and local mock servers. Real Codex
tests must use an isolated home and local model endpoint, not live threads or
account credentials. Keep private paths, logs, and identifiers out of commits.

Run `uv run python -m unittest discover -s tests -v`, `uvx ruff check .`,
`uvx ruff format --check .`, and `git diff --check`.
Use conventional commit headers that explain the purpose of the change.
