import pytest

from flashpoint.semantics.match_identity import identify, match_key


@pytest.mark.parametrize(
    ("args", "key"),
    [
        (("2026", "GACMP", "qm", 7, 1), "2026gacmp_qm7"),
        (("2026", "GACMP", "pm", 2, 1), "2026gacmp_pm2"),
        (("2026", "GACMP", "e", 10, None), "2026gacmp_e10"),
        (("2026", "GACMP", "qm", 7, 2), "2026gacmp_qm7r2"),
    ],
)
def test_match_key_format(args: tuple[str, str, str, int, int | None], key: str) -> None:
    assert match_key(*args) == key


def test_fms_identity() -> None:
    ident = identify("2026", fms_event="GACMP", fms_match_type=2, fms_match_number=7, fms_replay=1)
    assert (ident.key, ident.source, ident.kind, ident.warnings) == (
        "2026gacmp_qm7",
        "fms",
        "match",
        (),
    )


def test_filename_fallback() -> None:
    ident = identify("2026", file_event="GACMP", file_match_type="E", file_match_number=10)
    assert (ident.key, ident.source) == ("2026gacmp_e10", "filename")


def test_fms_wins_over_conflicting_filename() -> None:
    ident = identify(
        "2026", fms_event="GACMP", fms_match_type=2, fms_match_number=7,
        file_event="GACMP", file_match_type="Q", file_match_number=8,
    )  # fmt: skip
    assert ident.key == "2026gacmp_qm7"
    assert ident.warnings == ("match-identity-conflict",)


def test_non_match_log() -> None:
    ident = identify("2025", fms_event="GACMP")  # event set by the DS, but no match
    assert (ident.key, ident.kind, ident.source) == (None, "non-match", "none")


def test_unknown_season_gives_no_key() -> None:
    ident = identify("unknown", fms_event="GACMP", fms_match_type=2, fms_match_number=7)
    assert ident.key is None
    assert ident.warnings == ("no-season",)
