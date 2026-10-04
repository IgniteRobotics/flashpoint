import json
from datetime import date

from flashpoint.semantics.identity import DeviceSighting, InventoryEvent, resolve_identity
from flashpoint.semantics.robot_config import RobotConfig

SWAPS = ["2026-03-20", "2026-04-01"]
_SLOTS = [
    {"id": "drive-fl", "bus": "canivore", "model": "TalonFX", "can_id": 11},
    {
        "id": "hood",
        "bus": "rio",
        "model": "TalonFX",
        "can_id": 8,
        "swaps": ["2026-03-20", "2026-04-01"],
    },
]
ROBOT = RobotConfig.model_validate(
    {
        "robot": "bot",
        "season": 2026,
        "project": "P",
        "slot": [{**slot, "subsystem": "s", "role": "r"} for slot in _SLOTS],
    }
)
CAN = "6E9415C3394C485320202050101C18FF"


def _inv(ts: int, devices: list[tuple[str, str, int, str]]) -> InventoryEvent:
    payload = {
        "schema": 1,
        "devices": [{"bus": b, "model": m, "id": i, "serial": s} for b, m, i, s in devices],
    }
    return InventoryEvent(ts, json.dumps(payload))


def test_legacy_units_follow_swap_epochs() -> None:
    sightings = [DeviceSighting("rio", "TalonFX", 8), DeviceSighting(CAN, "TalonFX", 11)]
    for day, epoch in ((date(2026, 3, 1), 0), (date(2026, 3, 25), 1), (date(2026, 4, 9), 2)):
        result = resolve_identity("s1", ROBOT, day, sightings, [])
        units = {o.slot_id: o.unit_id for o in result.observations}
        assert units["hood"] == f"legacy:bot:hood:{epoch}"
        assert units["drive-fl"] == "legacy:bot:drive-fl:0"
        assert all(o.source == "legacy" for o in result.observations)


def test_inventory_serials_become_units() -> None:
    inv = _inv(100, [("rio", "Talon FX", 8, "ABC"), ("DriveTrain", "Talon FX", 11, "XYZ")])
    result = resolve_identity(
        "s1", ROBOT, date(2026, 4, 9), [DeviceSighting("rio", "TalonFX", 8)], [inv]
    )
    units = {o.slot_id: (o.unit_id, o.source) for o in result.observations}
    assert units == {"hood": ("ctre:ABC", "inventory"), "drive-fl": ("ctre:XYZ", "inventory")}


def test_mid_session_swap_splits_attribution() -> None:
    first = _inv(100, [("rio", "Talon FX", 8, "A")])
    second = _inv(500, [("rio", "Talon FX", 8, "B")])
    result = resolve_identity("s1", ROBOT, date(2026, 4, 9), [], [first, second])
    hood = sorted(
        ((o.from_ts_us, o.to_ts_us, o.unit_id) for o in result.observations if o.slot_id == "hood"),
        key=lambda row: -1 if row[0] is None else row[0],
    )
    assert hood == [(None, 500, "ctre:A"), (500, None, "ctre:B")]
    assert result.swaps == [("hood", 500, "ctre:A", "ctre:B")]


def test_invalid_inventory_falls_back_to_legacy() -> None:
    bad = InventoryEvent(100, json.dumps({"schema": 1, "error": "timed out"}))
    result = resolve_identity(
        "s1", ROBOT, date(2026, 4, 9), [DeviceSighting("rio", "TalonFX", 8)], [bad]
    )
    assert {o.unit_id for o in result.observations} == {"legacy:bot:hood:2"}


def test_unmapped_devices_reported() -> None:
    sightings = [DeviceSighting("rio", "TalonFX", 8), DeviceSighting("rio", "TalonFX", 99)]
    inv = _inv(100, [("rio", "Talon FX", 42, "Q")])
    result = resolve_identity("s1", ROBOT, date(2026, 4, 9), sightings, [inv])
    assert sorted(result.unmapped) == [
        ("rio", "TalonFX", 42, "inventory"),
        ("rio", "TalonFX", 99, "log"),
    ]
