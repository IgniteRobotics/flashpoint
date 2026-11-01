"""Which error reasons are the file's fault and which are the machine's.

Every reason raised as `HootError`/`WpilogError` in this package must be listed here
(a test scans the source). Unknown reasons count as `SUSPECT`.
"""

from enum import StrEnum


class ReasonKind(StrEnum):
    FILE = "file"  # the file is bad: quarantine now
    ENVIRONMENT = "environment"  # the machine failed: retry every run, no limit
    SUSPECT = "suspect"  # could be either: retry, quarantine on the 3rd attempt


SUSPECT_ATTEMPTS = 3

REASON_KINDS: dict[str, ReasonKind] = {
    "empty-file": ReasonKind.FILE,
    "invalid-header": ReasonKind.FILE,
    "too-old": ReasonKind.FILE,
    "unsupported-compliancy:": ReasonKind.FILE,
    "unsupported-version": ReasonKind.FILE,
    "no-signals-for-profile": ReasonKind.FILE,
    "owlet-not-cached": ReasonKind.ENVIRONMENT,
    "owlet-download-failed": ReasonKind.ENVIRONMENT,
    "owlet-checksum-mismatch": ReasonKind.ENVIRONMENT,
    "owlet-unavailable-for-platform": ReasonKind.ENVIRONMENT,
    "owlet-failed": ReasonKind.SUSPECT,
    "owlet-timeout": ReasonKind.SUSPECT,
    "internal-error:": ReasonKind.SUSPECT,
}  # a key ending in ":" matches every reason with that prefix


def _key(reason: str) -> str:
    prefix, sep, _ = reason.partition(":")
    if sep or f"{prefix}:" in REASON_KINDS:
        return f"{prefix}:"
    return reason


def is_listed(reason: str) -> bool:
    return _key(reason) in REASON_KINDS


def kind_of(reason: str) -> ReasonKind:
    return REASON_KINDS.get(_key(reason), ReasonKind.SUSPECT)
