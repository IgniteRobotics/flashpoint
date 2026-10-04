"""Command-line entry point. Subcommands arrive in P1 (ingest, doctor)."""

import sys
from collections.abc import Sequence

import flashpoint


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args in (["--version"], ["-V"]):
        print(f"flashpoint {flashpoint.__version__}")
    return 0
