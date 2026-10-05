from flashpoint.semantics.framing import Phase, frame, modes_from_ds

AUTO, TELEOP, DISABLED = "Autonomous", "Teleop", "Disabled"


def test_qual_match_phases() -> None:
    framing = frame(
        [
            (20, DISABLED),
            (127_500, AUTO),
            (134_940, DISABLED),
            (151_560, TELEOP),
            (291_580, DISABLED),
        ]
    )
    assert framing.phases == [
        Phase("pre", None, 127_500),
        Phase("auto", 127_500, 134_940),
        Phase("gap", 134_940, 151_560),
        Phase("teleop", 151_560, 291_580),
        Phase("post", 291_580, None),
    ]
    assert framing.match_start_us == 127_500


def test_never_enabled() -> None:
    framing = frame([(0, DISABLED)])
    assert framing.phases == [Phase("pre", None, None)]
    assert framing.match_start_us is None


def test_repeated_mode_samples_collapse() -> None:
    framing = frame([(0, DISABLED), (5, DISABLED), (10, TELEOP), (11, TELEOP), (20, DISABLED)])
    assert [p.name for p in framing.phases] == ["pre", "teleop", "post"]


def test_phase_at() -> None:
    framing = frame([(0, DISABLED), (100, AUTO), (200, DISABLED), (300, TELEOP), (400, DISABLED)])
    assert [framing.phase_at(t) for t in (50, 150, 250, 350, 450)] == [
        "pre",
        "auto",
        "gap",
        "teleop",
        "post",
    ]


def test_modes_from_driver_station_booleans() -> None:
    enabled = [(19, False), (127, True), (134, False), (152, True)]
    autonomous = [(19, False), (26, True), (152, False)]
    assert modes_from_ds(enabled, autonomous) == [
        (19, DISABLED), (127, AUTO), (134, DISABLED), (152, TELEOP),
    ]  # fmt: skip


def test_enabled_intervals_practice_toggles() -> None:
    """Practice toggles enable many times; every enabled run counts, Test mode included."""
    framing = frame(
        [
            (0, DISABLED),
            (100, TELEOP),
            (200, DISABLED),
            (300, "Test"),
            (350, DISABLED),
            (400, TELEOP),
            (450, DISABLED),
        ]
    )
    assert framing.enabled == [(100, 200), (300, 350), (400, 450)]


def test_enabled_intervals_match_merges_auto_into_teleop() -> None:
    framing = frame([(0, DISABLED), (100, AUTO), (115, TELEOP), (250, DISABLED)])
    assert framing.enabled == [(100, 250)]


def test_enabled_interval_open_at_end_of_log() -> None:
    framing = frame([(0, DISABLED), (100, TELEOP)])
    assert framing.enabled == [(100, None)]


def test_enabled_intervals_test_mode_only() -> None:
    framing = frame([(0, DISABLED), (100, "Test"), (200, DISABLED)])
    assert framing.match_start_us is None
    assert framing.enabled == [(100, 200)]
