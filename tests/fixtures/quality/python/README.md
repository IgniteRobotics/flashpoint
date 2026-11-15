Deliberately bad files, one per check-enforced Python rule. Each file's name is the rule it
breaks. `tests/test_quality.py` copies them out of the repo and asserts ruff reports that rule.
They are excluded from the repo's own lint, format, and type runs. Don't fix them.
