import numpy as np

from flashpoint.anomaly.settings import AnomalyConfig
from flashpoint.anomaly.siblings import Member, sibling_findings
from tests.anomaly.gold_builder import steady

CFG = AnomalyConfig()
DRIVE = ["drive-fl", "drive-fr", "drive-bl", "drive-br"]


def _members(values: dict[str, list[float]], units: dict[str, str] | None = None
             ) -> list[Member]:  # fmt: skip
    n = len(next(iter(values.values())))
    keys = [f"2026syn_qm{i + 1}" for i in range(n)]
    return [
        Member(slot, (units or {}).get(slot, f"ctre:{slot.upper()}"), keys,
               np.array(series, dtype=float))
        for slot, series in values.items()
    ]  # fmt: skip


def _run(members: list[Member], own_z: dict[str, dict[str, float]] | None = None):  # type: ignore[no-untyped-def]
    order = sorted({k for m in members for k in m.keys}, key=lambda k: int(k.split("qm")[1]))
    return sibling_findings("temp_max_c", members, order, CFG, own_z or {})


def test_one_drive_motor_runs_hot() -> None:
    values: dict[str, list[float]] = {
        slot: steady(40.0 + k * 0.2, 20, start=k) for k, slot in enumerate(DRIVE)
    }
    values["drive-bl"] = [v + (6.0 if i >= 15 else 0.0) for i, v in enumerate(values["drive-bl"])]
    findings = _run(_members(values))
    flagged = {f.subject: f for f in findings if f.flagged.any()}
    assert set(flagged) == {"ctre:DRIVE-BL"}
    hot = flagged["ctre:DRIVE-BL"]
    assert hot.method == "SIBLING" and hot.peer is not None
    assert hot.peer_name == "median of drive-fl, drive-fr, drive-br"
    assert np.flatnonzero(hot.normal.flagged).min() >= 15
    others = np.median([values[s][15] for s in ("drive-fl", "drive-fr", "drive-br")])
    assert hot.peer[15] == others


def test_whole_group_warms_together() -> None:
    values: dict[str, list[float]] = {
        slot: [v + (6.0 if i >= 15 else 0.0) for i, v in enumerate(steady(40.0, 20, start=k))]
        for k, slot in enumerate(DRIVE)
    }
    assert not any(f.flagged.any() for f in _run(_members(values)))


def test_missing_member_skips_the_match() -> None:
    values: dict[str, list[float]] = {
        slot: steady(40.0, 20, start=k) for k, slot in enumerate(DRIVE)
    }
    values["drive-fr"][7] = np.nan
    findings = _run(_members(values))
    assert all(np.isnan(f.values[7]) or not f.flagged[7] for f in findings)
    fl = next(f for f in findings if f.subject == "ctre:DRIVE-FL")
    assert np.isnan(fl.lo[7]) and np.isnan(fl.peer[7])


def test_pair_goes_to_the_member_further_from_its_own_baseline() -> None:
    pair = ["intake-extension", "intake-extension-follower"]
    values: dict[str, list[float]] = {pair[0]: steady(20.0, 20), pair[1]: steady(20.0, 20)}
    values[pair[1]] = [v + (8.0 if i >= 15 else 0.0) for i, v in enumerate(values[pair[1]])]
    keys = [f"2026syn_qm{i + 1}" for i in range(20)]
    own_z = {
        "ctre:INTAKE-EXTENSION": dict.fromkeys(keys, 0.3),
        "ctre:INTAKE-EXTENSION-FOLLOWER": {
            k: (8.0 if i >= 15 else 0.2) for i, k in enumerate(keys)
        },
    }
    findings = {f.subject: f for f in _run(_members(values), own_z)}
    assert findings["ctre:INTAKE-EXTENSION-FOLLOWER"].flagged.any()
    assert not findings["ctre:INTAKE-EXTENSION"].flagged.any()
    assert findings["ctre:INTAKE-EXTENSION-FOLLOWER"].peer_name == "intake-extension"


def test_history_follows_the_unit() -> None:
    values: dict[str, list[float]] = {
        slot: steady(40.0, 20, start=k) for k, slot in enumerate(DRIVE)
    }
    members = _members(values)
    swapped = members[0]
    members[0] = Member(swapped.slot, "ctre:NEW", swapped.keys[10:], swapped.values[10:])
    members.append(Member(swapped.slot, "ctre:OLD", swapped.keys[:10], swapped.values[:10]))
    findings = {f.subject: f for f in _run(members)}
    assert findings["ctre:NEW"].keys == swapped.keys[10:]
    assert findings["ctre:NEW"].baseline_n[4] == 4  # its own fifth group match
