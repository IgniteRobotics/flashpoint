"""Episodes: runs of flagged matches of one subject, method and metric. A gap of up to
`GAP` unflagged matches continues an episode; one more ends it. Ids are stable across runs."""

import hashlib
from dataclasses import dataclass

import numpy as np

GAP = 2  # unflagged matches an episode spans; the third one ends it
SENSITIVITIES = ("relaxed", "normal", "strict")


@dataclass(frozen=True)
class Episode:
    flagged: tuple[int, ...]

    @property
    def onset(self) -> int:
        return self.flagged[0]

    @property
    def last(self) -> int:
        return self.flagged[-1]

    @property
    def matches(self) -> int:
        return len(self.flagged)


def episodes(flagged: np.ndarray, gap: int = GAP) -> list[Episode]:
    found: list[list[int]] = []
    for i in np.flatnonzero(flagged).tolist():
        if found and i - found[-1][-1] - 1 <= gap:
            found[-1].append(i)
        else:
            found.append([i])
    return [Episode(tuple(run)) for run in found]


def anomaly_id(subject: str, method: str, metric: str, onset_match_key: str) -> str:
    key = f"{subject}|{method}|{metric}|{onset_match_key}"
    return hashlib.sha1(key.encode(), usedforsecurity=False).hexdigest()[:16]


def sensitivity(episode: Episode, normal: np.ndarray, severity: str) -> str:
    """The lowest sensitivity that shows it: relaxed hides LOW; strict adds what only the strict
    thresholds found."""
    if not normal[list(episode.flagged)].any():
        return "strict"
    return "normal" if severity == "LOW" else "relaxed"


def shown_at(tag: str, chosen: str) -> bool:
    return SENSITIVITIES.index(tag) <= SENSITIVITIES.index(chosen)
