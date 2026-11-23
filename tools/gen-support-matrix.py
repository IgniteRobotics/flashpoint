#!/usr/bin/env python3
"""Render the support matrix tables from docs/guide/support-matrix.toml (design D-e).

    python tools/gen-support-matrix.py          # rewrite the tables in support-matrix.md
    python tools/gen-support-matrix.py --check  # exit 1 naming each problem or stale table

Each table sits between `<!-- generated:matrix:<id> -->` and `<!-- /generated:matrix:<id> -->`
in docs/guide/support-matrix.md; the text around it is hand-written. The data is checked first:
every cell is one of the spec's six values, a "supported" cell names evidence that exists (a
corpus group, a path under tests/, or `outside:<yyyy-mm>` with an issue), a "rough" cell names
its workaround and issue, and a "not supported" cell links an issue (on it or on its row).
Problems name the table, the row and the column.
"""

import argparse
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "docs" / "guide" / "support-matrix.toml"
PAGE = REPO / "docs" / "guide" / "support-matrix.md"
CORPUS_MANIFEST = REPO / "tests" / "corpus" / "manifest.toml"
ISSUES = "https://github.com/IgniteRobotics/flashpoint/issues/"
BLOB = "https://github.com/IgniteRobotics/flashpoint/blob/main/"

SUPPORTED = ("supported", "supported (needs Phoenix Pro)")
VALUES = (*SUPPORTED, "rough", "not tested on real logs", "not tested", "not supported")
OUTSIDE = re.compile(r"^outside:\d{4}-\d{2}$")
BLOCK = re.compile(
    r"(<!-- generated:matrix:(?P<id>[a-z-]+) -->\n)(?P<body>.*?)"
    r"(<!-- /generated:matrix:(?P=id) -->)",
    re.DOTALL,
)


def corpus_groups(manifest: Path = CORPUS_MANIFEST) -> set[str]:
    with manifest.open("rb") as f:
        return {entry["group"] for entry in tomllib.load(f).get("file", [])}


def _evidence(cell: dict[str, Any]) -> list[str]:
    found = cell.get("evidence", [])
    return [found] if isinstance(found, str) else list(found)


def _evidence_problems(at: str, cell: dict[str, Any], repo: Path, groups: set[str]) -> list[str]:
    evidence = _evidence(cell)
    found = [] if evidence else [f"{at}: {cell['value']!r} names no evidence"]
    for item in evidence:
        if OUTSIDE.match(item):
            if "issue" not in cell:
                found.append(f"{at}: {item} needs an issue")
        elif item.startswith("tests/"):
            if not (repo / item).exists():
                found.append(f"{at}: evidence {item} doesn't exist")
        elif item not in groups:
            found.append(f"{at}: evidence {item!r} isn't a corpus group")
    return found


def _cell_problems(
    at: str, cell: dict[str, Any], row: dict[str, Any], repo: Path, groups: set[str]
) -> list[str]:
    value = cell.get("value")
    if value not in VALUES:
        return [f"{at}: {value!r} isn't one of {list(VALUES)}"]
    if value in SUPPORTED:
        return _evidence_problems(at, cell, repo, groups)
    if value == "rough" and not (cell.get("workaround") and cell.get("issue")):
        return [f"{at}: 'rough' needs a workaround and an issue"]
    if value == "not supported" and "issue" not in row and "issue" not in cell:
        return [f"{at}: 'not supported' needs an issue on the row or the cell"]
    return []


def problems(data: dict[str, Any], repo: Path, groups: set[str]) -> list[str]:
    """Every rule the data breaks, as `<table> / <row> / <column>: <problem>`."""
    found: list[str] = []
    for table in data.get("table", []):
        columns = {c["id"]: c["title"] for c in table["columns"]}
        for row in table.get("row", []):
            cells = row.get("cells", {})
            where = f"{table['id']} / {row['name']}"
            found += [
                f"{where} / {c}: not a column of this table" for c in cells.keys() - columns.keys()
            ]
            for column, title in columns.items():
                cell = cells.get(column)
                if cell is None:
                    found.append(f"{where} / {title}: no value")
                else:
                    found += _cell_problems(f"{where} / {title}", cell, row, repo, groups)
    return found


def _issue(number: int) -> str:
    return f"[#{number}]({ISSUES}{number})"


def _cell(cell: dict[str, Any]) -> str:
    text = str(cell["value"])
    if text == "rough":
        text += f": {cell['workaround']} ({_issue(cell['issue'])})"
    elif text == "not supported" and "issue" in cell:
        text += f" ({_issue(cell['issue'])})"
    details = []
    for item in _evidence(cell) if cell["value"] in SUPPORTED else []:
        if item.startswith("tests/"):
            details.append(f"[test]({BLOB}{item})")
        elif OUTSIDE.match(item):
            details.append(f"outside team log, {item.split(':', 1)[1]} ({_issue(cell['issue'])})")
        else:
            details.append(f"corpus `{item}`")
    if cell.get("note"):
        details.append(cell["note"])
    if details:
        text += "<br><small>" + "; ".join(details) + "</small>"
    return text.replace("|", "\\|")


def tables(data: dict[str, Any]) -> dict[str, str]:
    """Each table's Markdown, by id."""
    out = {}
    for table in data.get("table", []):
        columns = table["columns"]
        lines = [
            "| " + " | ".join([table["row_title"], *(c["title"] for c in columns)]) + " |",
            "|" + "---|" * (len(columns) + 1),
        ]
        for row in table.get("row", []):
            name = row["name"] + (f" ({_issue(row['issue'])})" if "issue" in row else "")
            cells = [_cell(row["cells"][c["id"]]) for c in columns]
            lines.append("| " + " | ".join([name, *cells]) + " |")
        out[table["id"]] = "\n".join(lines) + "\n"
    return out


def stale(text: str, generated: dict[str, str]) -> list[str]:
    blocks = {m["id"]: m["body"] for m in BLOCK.finditer(text)}
    found = [f"{name}: no generated:matrix block" for name in generated if name not in blocks]
    found += [
        f"{name}: out of date" for name, body in generated.items() if blocks.get(name, body) != body
    ]
    found += [f"{name}: block has no table in the data" for name in blocks if name not in generated]
    return found


def render(text: str, generated: dict[str, str]) -> str:
    return BLOCK.sub(lambda m: m[1] + generated.get(m["id"], m["body"]) + m[4], text)


def load(path: Path = DATA) -> dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="report problems; write nothing")
    args = parser.parse_args(argv)
    data = load()
    found = problems(data, REPO, corpus_groups())
    if found:
        print("\n".join(found), file=sys.stderr)
        return 1
    text = PAGE.read_text(encoding="utf-8")
    generated = tables(data)
    if args.check:
        out_of_date = stale(text, generated)
        if out_of_date:
            print(
                "\n".join([*out_of_date, "run: python tools/gen-support-matrix.py"]),
                file=sys.stderr,
            )
        return 1 if out_of_date else 0
    missing = [name for name in generated if f"<!-- generated:matrix:{name} -->" not in text]
    if missing:
        print(f"{PAGE.name} has no block for: {', '.join(missing)}", file=sys.stderr)
        return 1
    PAGE.write_text(render(text, generated), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
