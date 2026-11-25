"""Canonical match keys: `<season><event>_<pm|qm|e><n>[r<replay>]` (e.g. 2026gacmp_qm7)."""

from dataclasses import dataclass

FMS_LEVEL = {1: "pm", 2: "qm", 3: "e"}  # FMSInfo/MatchType, /DriverStation/MatchType
# FRC_..._<EVENT>_<P|Q|E><n>.wpilog, akit_..._<event>_<p|q|e><n>.wpilog, <EVENT>_<P|Q|E><n>_….hoot
FILE_LEVEL = {"P": "pm", "Q": "qm", "E": "e"}


@dataclass(frozen=True)
class MatchIdentity:
    key: str | None
    source: str  # fms | filename | hoot-filename | none
    kind: str  # match | non-match
    warnings: tuple[str, ...] = ()


def match_key(season: str, event: str, level: str, number: int, replay: int | None) -> str:
    suffix = f"r{replay}" if replay and replay > 1 else ""
    return f"{season}{event.lower()}_{level}{number}{suffix}"


def _file_match(
    event: str | None, match_type: str | None, number: int | None
) -> tuple[str, str, int] | None:
    level = FILE_LEVEL.get((match_type or "").upper())
    return (event, level, number) if event and number and level else None


def identify(
    season: str,
    fms_event: str | None = None,
    fms_match_type: int | None = None,
    fms_match_number: int | None = None,
    fms_replay: int | None = None,
    file_event: str | None = None,
    file_match_type: str | None = None,
    file_match_number: int | None = None,
    hoot_event: str | None = None,
    hoot_match_type: str | None = None,
    hoot_match_number: int | None = None,
    hoot_conflict: bool = False,
) -> MatchIdentity:
    """In-log FMS data first, then the wpilog filename, then the hoot label (the earliest hoot
    group's; `hoot_conflict` when the session's groups disagree)."""
    fms = (
        (fms_event, FMS_LEVEL[fms_match_type], fms_match_number)
        if fms_event and fms_match_number and fms_match_type in FMS_LEVEL
        else None
    )
    file = _file_match(file_event, file_match_type, file_match_number)
    hoot = None if fms or file else _file_match(hoot_event, hoot_match_type, hoot_match_number)
    ranked = ((fms, "fms"), (file, "filename"), (hoot, "hoot-filename"))
    chosen, source = next(((found, name) for found, name in ranked if found), (None, "none"))
    if chosen is None:
        return MatchIdentity(None, "none", "non-match")
    warnings: list[str] = []
    if fms and file and (fms[0].lower(), fms[1], fms[2]) != (file[0].lower(), file[1], file[2]):
        warnings.append("match-identity-conflict")
    if hoot and hoot_conflict:
        warnings.append("match-identity-conflict")
    if not season.isdigit():
        return MatchIdentity(None, source, "match", (*warnings, "no-season"))
    event, level, number = chosen
    replay = fms_replay if source == "fms" else None
    return MatchIdentity(
        match_key(season, event, level, number, replay), source, "match", tuple(warnings)
    )
