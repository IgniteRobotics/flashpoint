import re
from pathlib import Path

import pytest

from flashpoint.readers import reasons
from flashpoint.readers.reasons import ReasonKind, kind_of

READERS = Path(reasons.__file__).parent


@pytest.mark.parametrize(
    "reason",
    [
        "empty-file",
        "invalid-header",
        "too-old",
        "unsupported-compliancy:19",
        "unsupported-version",
        "no-signals-for-profile",
    ],
)
def test_file_reasons(reason: str) -> None:
    assert kind_of(reason) is ReasonKind.FILE


@pytest.mark.parametrize(
    "reason",
    [
        "owlet-not-cached",
        "owlet-download-failed",
        "owlet-checksum-mismatch",
        "owlet-unavailable-for-platform",
    ],
)
def test_environment_reasons(reason: str) -> None:
    assert kind_of(reason) is ReasonKind.ENVIRONMENT


@pytest.mark.parametrize(
    "reason", ["owlet-failed", "owlet-timeout", "internal-error:KeyError", "never-heard-of-it"]
)
def test_suspect_reasons(reason: str) -> None:
    assert kind_of(reason) is ReasonKind.SUSPECT


def test_every_reader_reason_is_classified() -> None:
    raised: set[str] = set()
    for source in READERS.glob("*.py"):
        text = source.read_text()
        raised |= set(re.findall(r'(?:HootError|WpilogError)\(\s*f?"([a-z-]+)', text))
    assert raised, "the scan found no reasons; the pattern is broken"
    missing = {r for r in raised if not reasons.is_listed(r)}
    assert not missing
