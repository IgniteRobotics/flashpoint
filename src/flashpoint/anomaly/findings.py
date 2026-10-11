"""A judged series: one subject's values for one method and metric, in that subject's own
match order, with the per-match flags at strict and normal thresholds."""

from dataclasses import dataclass

import numpy as np

from flashpoint.anomaly.shapes import Flags


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

    @property
    def flagged(self) -> np.ndarray:
        either: np.ndarray = self.strict.flagged | self.normal.flagged
        return either
