"""Canonical match keys: `<season><event>_<pm|qm|e><n>[r<replay>]` (e.g. 2026gacmp_qm7)."""

from dataclasses import dataclass

FMS_LEVEL = {1: "pm", 2: "qm", 3: "e"}  # FMSInfo/MatchType
FILE_LEVEL = {"P": "pm", "Q": "qm", "E": "e"}  # FRC_..._<EVENT>_<P|Q|E><n>.wpilog


@dataclass(frozen=True)
class MatchIdentity:
    key: str | None
    source: str  # fms | filename | none
    kind: str  # match | non-match
    warnings: tuple[str, ...] = ()


def match_key(season: str, event: str, level: str, number: int, replay: int | None) -> str:
    suffix = f"r{replay}" if replay and replay > 1 else ""
    return f"{season}{event.lower()}_{level}{number}{suffix}"


def identify(
    season: str,
    fms_event: str | None = None,
    fms_match_type: int | None = None,
    fms_match_number: int | None = None,
    fms_replay: int | None = None,
    file_event: str | None = None,
    file_match_type: str | None = None,
    file_match_number: int | None = None,
) -> MatchIdentity:
    """In-log FMS data first; the filename only when FMS has no match number."""
    fms = (
        (fms_event, FMS_LEVEL[fms_match_type], fms_match_number)
        if fms_event and fms_match_number and fms_match_type in FMS_LEVEL
        else None
    )
    file = (
        (file_event, FILE_LEVEL[file_match_type], file_match_number)
        if file_event and file_match_number and file_match_type in FILE_LEVEL
        else None
    )
    chosen, source = (fms, "fms") if fms else (file, "filename") if file else (None, "none")
    if chosen is None:
        return MatchIdentity(None, "none", "non-match")
    warnings: list[str] = []
    if fms and file and (fms[0].lower(), fms[1], fms[2]) != (file[0].lower(), file[1], file[2]):
        warnings.append("match-identity-conflict")
    if not season.isdigit():
        return MatchIdentity(None, source, "match", (*warnings, "no-season"))
    event, level, number = chosen
    replay = fms_replay if source == "fms" else None
    return MatchIdentity(
        match_key(season, event, level, number, replay), source, "match", tuple(warnings)
    )
