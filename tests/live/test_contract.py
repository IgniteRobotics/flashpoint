import pytest

from flashpoint.live import contract
from flashpoint.live.contract import BusTopic, DeviceTopic, TopTopic


def test_version_one_and_root() -> None:
    assert contract.CONTRACT_VERSION == 1
    assert contract.ROOT == "/Flashpoint/Live/CANHealth/"


def test_topic_types() -> None:
    assert contract.TOP_FIELDS == {"Version": "int", "Project": "string", "Enabled": "boolean"}
    assert contract.DEVICE_FIELDS == {
        "Connected": "boolean",
        "Dropouts": "int",
        "MaxAgeMs": "double",
        "Resets": "int",
        "Faults": "int",
        "MinSupplyV": "double",
    }
    assert contract.BUS_FIELDS == {
        "Utilization": "double",
        "BusOffCount": "int",
        "TxFullCount": "int",
        "ReceiveErrorCount": "int",
        "TransmitErrorCount": "int",
    }


def test_counts_are_the_running_totals() -> None:
    assert contract.DEVICE_TOTALS == ("Dropouts", "Resets")
    assert contract.BUS_TOTALS == (
        "BusOffCount", "TxFullCount", "ReceiveErrorCount", "TransmitErrorCount"
    )


def test_fault_bits() -> None:
    assert contract.FAULT_BITS == {
        0: "under-voltage",
        1: "boot during enable",
        2: "hardware",
        3: "bridge brownout",
        4: "bad magnet",
    }
    assert contract.fault_names(0b10010) == ["boot during enable", "bad magnet"]
    assert contract.fault_names(0) == []


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("/Flashpoint/Live/CANHealth/Version", TopTopic("Version")),
        ("/Flashpoint/Live/CANHealth/Project", TopTopic("Project")),
        ("/Flashpoint/Live/CANHealth/Enabled", TopTopic("Enabled")),
        (
            "/Flashpoint/Live/CANHealth/DriveTrain/TalonFX-11/Dropouts",
            DeviceTopic("DriveTrain", "TalonFX", 11, "Dropouts"),
        ),
        (
            "/Flashpoint/Live/CANHealth/rio/CANcoder-3/MinSupplyV",
            DeviceTopic("rio", "CANcoder", 3, "MinSupplyV"),
        ),
        (
            "/Flashpoint/Live/CANHealth/DriveTrain/Bus/Utilization",
            BusTopic("DriveTrain", "Utilization"),
        ),
        ("/Flashpoint/Live/CANHealth/rio/Bus/BusOffCount", BusTopic("rio", "BusOffCount")),
    ],
)
def test_parse_known_topics(name: str, expected: object) -> None:
    assert contract.parse_topic(name) == expected


@pytest.mark.parametrize(
    "name",
    [
        "/Flashpoint/CANBus/rio/Utilization",  # the wpilog tree, not the live one
        "/Flashpoint/Live/CANHealth/Unknown",
        "/Flashpoint/Live/CANHealth/rio/TalonFX-11/Temperature",  # not a contract field
        "/Flashpoint/Live/CANHealth/rio/TalonFX/Dropouts",  # no CAN id
        "/Flashpoint/Live/CANHealth/rio/TalonFX-x/Dropouts",
        "/Flashpoint/Live/CANHealth/rio/TalonFX-63/Dropouts",  # CAN ids are 0-62
        "/Flashpoint/Live/CANHealth/rio/Bus/Dropouts",  # device field under Bus
        "/Flashpoint/Live/CANHealth/rio/a/b/Dropouts",
        "/Flashpoint/Live/CANHealth/",
        "",
    ],
)
def test_parse_rejects_other_topics(name: str) -> None:
    assert contract.parse_topic(name) is None


def test_device_name_round_trip() -> None:
    assert contract.parse_device("TalonFX-11") == ("TalonFX", 11)
    assert contract.parse_device("Pigeon2-10") == ("Pigeon2", 10)
    assert contract.parse_device("TalonFX11") is None
    assert contract.device_name("TalonFX", 11) == "TalonFX-11"


def test_topic_names_cover_every_field() -> None:
    names = contract.topic_names("DriveTrain", ["TalonFX-11"])
    assert names == sorted(names)
    assert "/Flashpoint/Live/CANHealth/Version" in names
    assert "/Flashpoint/Live/CANHealth/DriveTrain/TalonFX-11/Faults" in names
    assert "/Flashpoint/Live/CANHealth/DriveTrain/Bus/TxFullCount" in names
    assert len(names) == len(contract.TOP_FIELDS) + len(contract.DEVICE_FIELDS) + len(
        contract.BUS_FIELDS
    )
    assert all(contract.parse_topic(n) is not None for n in names)
