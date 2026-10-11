"""SIBLING: a unit against the other members of its sibling group (robot config `[[siblings]]`).

In each match the whole group played, a member's difference is its value minus the median of
the other members. Each unit's difference series gets the per-unit baseline treatment, and a
match is flagged when the difference's z reaches `sibling.z` in at least `need` of the unit's
last `of` group matches. In a group of two, the differences mirror each other, so only the
member further from its own baseline keeps the flag.
"""

from dataclasses import dataclass, field

import numpy as np

from flashpoint.anomaly.baseline import baseline_at
from flashpoint.anomaly.findings import Finding
from flashpoint.anomaly.settings import AnomalyConfig
from flashpoint.anomaly.shapes import Flags


@dataclass(frozen=True)
class Member:
    slot: str
    unit: str
    keys: list[str]  # the unit's matches in this slot, in order
    values: np.ndarray  # NaN where missing


@dataclass
class _Diffs:
    member: Member
    keys: list[str] = field(default_factory=list)
    d: list[float] = field(default_factory=list)
    peer: list[float] = field(default_factory=list)


def _flags(z: np.ndarray, threshold: float, need: int, of: int) -> Flags:
    flags = Flags.empty(z.size)
    beyond = np.abs(np.nan_to_num(z)) >= threshold
    for k in range(z.size):
        window = range(max(0, k - of + 1), k + 1)
        if sum(beyond[j] for j in window) >= need:
            for j in window:
                if beyond[j]:
                    flags.mark(j, float(z[j]), k)
    return flags


def _scatter(size: int, index: list[int], values: np.ndarray, fill: float = np.nan) -> np.ndarray:
    out = np.full(size, fill, dtype=float)
    out[index] = values
    return out


def sibling_findings(
    metric: str,
    members: list[Member],
    order: list[str],
    cfg: AnomalyConfig,
    own_z: dict[str, dict[str, float]],
) -> list[Finding]:
    slots = list(dict.fromkeys(m.slot for m in members))
    if len(slots) < 2:
        return []
    at: dict[tuple[str, str], tuple[Member, float]] = {}
    for m in members:
        for key, value in zip(m.keys, m.values, strict=True):
            if not np.isnan(value):
                at[(m.slot, key)] = (m, float(value))
    diffs: dict[tuple[str, str], _Diffs] = {}
    for key in order:
        present = [at.get((slot, key)) for slot in slots]
        if any(p is None for p in present):
            continue  # a member is missing: skip the match for the group
        values = [p[1] for p in present if p is not None]
        for at_slot, p in enumerate(present):
            assert p is not None
            median = float(np.median(values[:at_slot] + values[at_slot + 1 :]))
            series = diffs.setdefault((p[0].slot, p[0].unit), _Diffs(p[0]))
            series.keys.append(key)
            series.d.append(p[1] - median)
            series.peer.append(median)

    floor = cfg.baseline.floors[metric]
    normal_z, strict_z = cfg.sibling.z, cfg.strict.sibling_z
    judged: dict[tuple[str, str], tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = {}
    flagged: dict[tuple[str, str], tuple[Flags, Flags]] = {}
    for ident, series in diffs.items():
        d = np.array(series.d)
        z, center, spread = (
            np.full(d.size, np.nan),
            np.full(d.size, np.nan),
            np.full(d.size, np.nan),
        )
        n = np.zeros(d.size, dtype=np.int64)
        for k in range(d.size):
            b = baseline_at(d, k, cfg.baseline.window, floor)
            if b is None:
                continue
            center[k], spread[k], n[k] = b.center, b.spread, b.n
            if b.n >= cfg.baseline.min_baseline:
                z[k] = (d[k] - b.center) / b.spread
        judged[ident] = (z, center, spread, n)
        flagged[ident] = (
            _flags(z, strict_z, cfg.strict.persist, cfg.sibling.of),
            _flags(z, normal_z, cfg.sibling.need, cfg.sibling.of),
        )
    if len(slots) == 2:
        _pair_rule(diffs, flagged, own_z)

    findings = []
    for ident, series in diffs.items():
        member = series.member
        z, center, spread, n = judged[ident]
        strict, normal = flagged[ident]
        index = [member.keys.index(k) for k in series.keys]
        size = len(member.keys)
        peer = np.array(series.peer)
        others = [s for s in slots if s != member.slot]
        findings.append(
            Finding(
                subject=member.unit, method="SIBLING", metric=metric, keys=list(member.keys),
                values=member.values,
                strict=_scatter_flags(size, index, strict),
                normal=_scatter_flags(size, index, normal),
                lo=_scatter(size, index, peer + center - normal_z * spread),
                hi=_scatter(size, index, peer + center + normal_z * spread),
                baseline_n=_scatter(size, index, n.astype(float), 0).astype(np.int64),
                peer=_scatter(size, index, peer),
                peer_name=others[0] if len(others) == 1 else f"median of {', '.join(others)}",
            )
        )  # fmt: skip
    return findings


def _scatter_flags(size: int, index: list[int], flags: Flags) -> Flags:
    out = Flags.empty(size)
    for k, i in enumerate(index):
        if flags.flagged[k]:
            known = index[int(flags.known_at[k])]
            out.mark(i, float(flags.score[k]), known)
    return out


def _pair_rule(
    diffs: dict[tuple[str, str], _Diffs],
    flagged: dict[tuple[str, str], tuple[Flags, Flags]],
    own_z: dict[str, dict[str, float]],
) -> None:
    """Of two mirrored members flagged in the same match, unflag the one closer to its own
    baseline (both, when neither has one)."""
    position = {ident: {k: n for n, k in enumerate(s.keys)} for ident, s in diffs.items()}
    idents = list(diffs)
    for a, b in ((x, y) for i, x in enumerate(idents) for y in idents[i + 1 :]):
        if a[0] == b[0]:
            continue  # two units of one slot never play the same match
        for key in set(position[a]) & set(position[b]):
            za = abs(own_z.get(a[1], {}).get(key, np.nan))
            zb = abs(own_z.get(b[1], {}).get(key, np.nan))
            losers = (
                [a, b] if np.isnan(za) and np.isnan(zb)
                else [b] if np.isnan(zb) or za >= zb
                else [a]
            )  # fmt: skip
            for loser in losers:
                for flags in flagged[loser]:
                    j = position[loser][key]
                    flags.flagged[j] = False
