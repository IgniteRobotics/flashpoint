"""Download names for raw logs (shared by the Replay build, the server, and History)."""

import re


def safe_name(name: str) -> str:
    """A file name safe on every OS (the original name is shown as text, not used as a path)."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._") or "file"
    return cleaned[:150]


def download_name(match_key: str, part: str, original: str) -> str:
    """`<match_key>__<bus or wpilog>__<original name>`, so the match is recognisable."""
    return safe_name(f"{match_key}__{part}__{original}")
