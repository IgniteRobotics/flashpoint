Deliberately bad app scripts, one per check-enforced frontend rule. Each file's name is the rule
it breaks. `tests/test_quality.py` copies them out of the repo and asserts the frontend linter,
formatter, or static-lint check reports that rule. Don't fix them.
