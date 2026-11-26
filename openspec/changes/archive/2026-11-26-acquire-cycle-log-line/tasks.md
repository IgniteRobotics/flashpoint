## 1. Tests first

- [x] 1.1 In `tests/acquire/test_watch.py`, add tests that build a `CycleResult` and capture the `cycle done` line at info level (`caplog`, logger `flashpoint.acquire.watch`), one per spec scenario: robot pull of three logs, mixed results, same results in a different finish order, nothing to do, stopped cycle. Also assert the line has no `{`, `}`, `[`, `]`, or `'`. Verify they fail against the current code with `PYENV_VERSION=flashpoint poetry run pytest tests/acquire/test_watch.py -k cycle_done`.

## 2. Log line

- [x] 2.1 In `src/flashpoint/acquire/watch.py`, write the transfer and inbox counts as `<n> <status>` pairs joined by `, `, ordered by the status enums' definition order (`TransferStatus`, `InboxOutcome`), keeping `none` / `empty` and the source list as they are. Verify the 1.1 tests pass.
- [x] 2.2 Run `PYENV_VERSION=flashpoint poetry run pytest tests/acquire`, `poetry run ruff check .`, and `poetry run mypy`; all pass.

## 3. Docs

- [x] 3.1 Update the example under "Check it worked" in `docs/guide/how-to/auto-import.md` to `cycle done: robot 10.68.29.2 ok, volume ROBOT-USB ok; transfers 3 verified; inbox 3 success`, and check the `verified` / `size-verified` / `retry` sentence near the end of "Check the status" still reads right. Verify with `PYENV_VERSION=flashpoint poetry run pytest tests/test_docs*.py`.
- [x] 3.2 Confirm `docs/rewrite` needs no update (no decision or design changes; the roadmap does not quote the log line) and note that in the PR.

## 4. Field check

- [ ] 4.1 [HUMAN] Run `flashpoint acquire` once against a robot or a USB stick and confirm the `cycle done` line reads like the spec (no dict reprs).
