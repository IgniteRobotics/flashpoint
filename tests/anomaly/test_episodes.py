import numpy as np

from flashpoint.anomaly.episodes import anomaly_id, episodes, sensitivity


def _flags(size: int, flagged: list[int]) -> np.ndarray:
    out = np.zeros(size, bool)
    out[flagged] = True
    return out


def test_episode_continues_across_a_short_gap() -> None:
    # matches 21, 22, 23, 25 (1-based) are indices 20, 21, 22, 24
    (episode,) = episodes(_flags(30, [20, 21, 22, 24]))
    assert (episode.onset, episode.last, episode.matches) == (20, 24, 4)
    assert episode.flagged == (20, 21, 22, 24)


def test_episode_ends_after_three_unflagged() -> None:
    first, second = episodes(_flags(30, [20, 21, 22, 26]))
    assert (first.onset, first.last, second.onset) == (20, 22, 26)
    keys = [f"2026syn_qm{i + 1}" for i in range(30)]
    a = anomaly_id("ctre:X", "STEP", "temp_max_c", keys[first.onset])
    b = anomaly_id("ctre:X", "STEP", "temp_max_c", keys[second.onset])
    assert a != b


def test_ids_are_stable_and_short() -> None:
    a = anomaly_id("ctre:X", "STEP", "temp_max_c", "2026syn_qm21")
    assert a == anomaly_id("ctre:X", "STEP", "temp_max_c", "2026syn_qm21")
    assert len(a) == 16 and int(a, 16) >= 0
    assert a != anomaly_id("ctre:Y", "STEP", "temp_max_c", "2026syn_qm21")
    assert a != anomaly_id("ctre:X", "DRIFT", "temp_max_c", "2026syn_qm21")
    assert a != anomaly_id("ctre:X", "STEP", "temp_mean_c", "2026syn_qm21")


def test_no_flags_no_episodes() -> None:
    assert episodes(np.zeros(10, bool)) == []


def test_sensitivity_tags() -> None:
    strict = _flags(30, [20, 21, 22, 23, 24])
    (episode,) = episodes(strict)
    assert sensitivity(episode, _flags(30, []), "LOW") == "strict"
    assert sensitivity(episode, _flags(30, [23]), "LOW") == "normal"
    assert sensitivity(episode, _flags(30, [23]), "MED") == "relaxed"
    assert sensitivity(episode, _flags(30, [5]), "HIGH") == "strict"  # outside the episode
