#!/usr/bin/env python3
"""Generate the option tables in the CLI reference from the CLI itself (design D-e).

    python tools/gen-cli-reference.py          # rewrite docs/guide/reference/cli.md's tables
    python tools/gen-cli-reference.py --check  # exit 1 naming each table that's out of date

Each subcommand's table sits between `<!-- generated:cli:<name> -->` and
`<!-- /generated:cli:<name> -->`; the purpose, notes and examples around it are hand-written.
The option column is what `--help` shows (argparse's own formatting). An option with no help
text fails the generation, naming the subcommand and the option.
"""

import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
REFERENCE = REPO / "docs" / "guide" / "reference" / "cli.md"
HEADER = "| Option | What it does | Default |\n|---|---|---|\n"
BLOCK = re.compile(
    r"(<!-- generated:cli:(?P<name>[a-z-]+) -->\n)(?P<body>.*?)(<!-- /generated:cli:(?P=name) -->)",
    re.DOTALL,
)
ROW_OPTION = re.compile(r"^\| `([^` ]+)")


def _subcommands(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    actions = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
    return {name: sub for action in actions for name, sub in action.choices.items()}


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def _default(action: argparse.Action) -> str:
    value = action.default
    if value is None or value is False or value == argparse.SUPPRESS or value == []:
        return ""
    if isinstance(value, list | tuple):
        value = " ".join(str(v) for v in value)
    return f"`{value}`"


def tables(parser: argparse.ArgumentParser) -> dict[str, str]:
    """Each subcommand's options table, by name. Raises ValueError naming every option with no
    help text."""
    result: dict[str, str] = {}
    missing: list[str] = []
    for name, sub in _subcommands(parser).items():
        formatter = sub._get_formatter()
        rows = []
        for action in sub._actions:
            if isinstance(action, argparse._HelpAction):
                continue
            invocation = formatter._format_action_invocation(action)
            if not action.help or action.help == argparse.SUPPRESS:
                option = action.option_strings[-1] if action.option_strings else action.dest
                missing.append(f"{name}: {option} has no help text")
                continue
            help_text = _cell(formatter._expand_help(action))  # %(default)s as --help shows it
            default = _default(action)
            rows.append(f"| `{invocation}` | {help_text} |{f' {default} ' if default else ' '}|\n")
        result[name] = HEADER + "".join(rows)
    if missing:
        raise ValueError("; ".join(missing))
    return result


def _blocks(text: str) -> dict[str, str]:
    return {m["name"]: m["body"] for m in BLOCK.finditer(text)}


def _options(table: str) -> list[str]:
    return [m[1] for line in table.splitlines() if (m := ROW_OPTION.match(line))]


def problems(text: str, generated: dict[str, str]) -> list[str]:
    """What's out of date in a reference page, one line per subcommand."""
    blocks = _blocks(text)
    found = []
    for name, table in generated.items():
        if name not in blocks:
            found.append(f"{name}: no generated table")
            continue
        if blocks[name] == table:
            continue
        have, want = _options(blocks[name]), _options(table)
        added = [o for o in want if o not in have]
        removed = [o for o in have if o not in want]
        found += [f"{name}: {o} isn't in the reference" for o in added]
        found += [f"{name}: {o} is in the reference but not the CLI" for o in removed]
        if not added and not removed:
            found.append(f"{name}: the table is out of date (help text or a default changed)")
    found += [f"{name}: generated table for a subcommand the CLI doesn't have"
              for name in blocks if name not in generated]  # fmt: skip
    return found


def render(text: str, generated: dict[str, str]) -> str:
    """The page with every generated block replaced; ValueError names a subcommand with none."""
    missing = [name for name in generated if name not in _blocks(text)]
    if missing:
        raise ValueError(f"no <!-- generated:cli:NAME --> block for: {', '.join(missing)}")
    return BLOCK.sub(lambda m: m[1] + generated[m["name"]] + m[4], text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="report, don't write")
    args = parser.parse_args(argv)
    sys.path.insert(0, str(REPO / "src"))
    from flashpoint.cli import _parser

    try:
        generated = tables(_parser())
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1
    text = REFERENCE.read_text(encoding="utf-8")
    if args.check:
        found = problems(text, generated)
        for line in found:
            print(line, file=sys.stderr)
        return 1 if found else 0
    REFERENCE.write_text(render(text, generated), encoding="utf-8")
    print(f"wrote {len(generated)} tables in {REFERENCE.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
