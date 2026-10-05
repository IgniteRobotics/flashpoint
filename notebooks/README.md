# Notebooks

Exploration space for students. The supported views are Replay and History in `flashpoint serve`; notebooks are for questions those views don't answer yet.

marimo is **not** a Flashpoint dependency, so install it next to Flashpoint yourself:

```sh
pipx inject flashpoint marimo        # or, in a dev checkout: poetry run pip install marimo
marimo edit notebooks/unit-trends.py
```

Each notebook reads the lake read-only through `flashpoint.views.queries.HistoryQueries`, the same parameterised DuckDB queries behind History's API. Filters run inside DuckDB, so nothing loads whole tables (#35). Set the lake with `FLASHPOINT_LAKE` or edit the path cell.

| Notebook | What it shows |
|---|---|
| `unit-trends.py` | One metric per match for each unit, a unit's odometry totals, and its lifeline |

To add a notebook, copy `unit-trends.py`. Keep notebooks read-only: never write to the lake from one.
