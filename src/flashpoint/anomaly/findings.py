"""A judged series: one subject's values for one method and metric, in that subject's own
match order, with the per-match flags at strict and normal thresholds."""

from dataclasses import dataclass

import numpy as np

NEVER = np.iinfo(np.int64).max


@dataclass
class Flags:
    """Per index: flagged, the signed z that flagged it, and the index it was known at."""

    flagged: np.ndarray
    score: np.ndarray
    known_at: np.ndarray

    @classmethod
    def empty(cls, size: int) -> "Flags":
        return cls(np.zeros(size, bool), np.zeros(size), np.full(size, NEVER, dtype=np.int64))

    def mark(self, j: int, z: float, at: int) -> None:
        self.flagged[j] = True
        if abs(z) > abs(self.score[j]):
            self.score[j] = z
        self.known_at[j] = min(self.known_at[j], at)


@dataclass
class Finding:
    subject: str  # a unit id, or "power:<robot>"
    method: str
    metric: str
    keys: list[str]  # the subject's matches, in order
    values: np.ndarray  # NaN where the metric is missing
    strict: Flags  # a superset of `normal`
    normal: Flags
    lo: np.ndarray  # the expected band per match (NaN: none)
    hi: np.ndarray
    baseline_n: np.ndarray  # baseline size per match (0 for SPEC)
    levels: list[str] | None = None  # SPEC: "", "MED" or "HIGH" per match
    peer: np.ndarray | None = None  # SIBLING: the median of the other members
    peer_name: str | None = None
    expected: str | None = None  # SPEC: the limit, e.g. "< 65 °C"
    reasons: list[str] | None = None  # MODEL: per match, the features furthest from baseline

    @property
    def flagged(self) -> np.ndarray:
        either: np.ndarray = self.strict.flagged | self.normal.flagged
        return either
