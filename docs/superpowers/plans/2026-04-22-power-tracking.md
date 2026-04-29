# Power Tracking: Supply Watts per RPS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `RotorVelocity` (RPS) as a first-class field in the motor data pipeline and expose a new per-motor plot of supply watts per RPS over time.

**Architecture:** Expand `MOTOR_COL_PATTERN` in `loader.py` to capture `Velocity` signals from Phoenix6 TalonFX devices, store it as `rotor_velocity` on `MotorData`, wire it through `compute_motor_data` in `analyzer.py`, and render it as a smoothed W/RPS time series in a new `plot_watts_per_rps` function in `plotter.py`. The ratio is computed at render time — not stored on the model.

**Tech Stack:** Python 3.11+, numpy, pandas, matplotlib, pytest

---

## File Map

| File | Change |
|------|--------|
| `utils/loader.py` | Expand `MOTOR_COL_PATTERN` to include `Velocity` |
| `utils/models.py` | Add `rotor_velocity: np.ndarray \| None = None` to `MotorData` |
| `utils/analyzer.py` | Capture `Velocity` in `compute_motor_data`; `compute_totals` leaves `rotor_velocity=None` |
| `utils/plotter.py` | Add `WATTS_PER_RPS_SMOOTH_S`, `VEL_MIN_RPS` constants + `plot_watts_per_rps` function |
| `tests/utils/conftest.py` | Add `simple_match_with_velocity` fixture |
| `tests/utils/test_models.py` | Cover `rotor_velocity` field |
| `tests/utils/test_analyzer.py` | Cover velocity capture + totals leaves it `None` |
| `tests/utils/test_plotter.py` | Cover `plot_watts_per_rps` |

---

## Task 1: Expand MOTOR_COL_PATTERN to capture Velocity

**Files:**
- Modify: `utils/loader.py`
- Test: `tests/utils/test_loader.py`

- [ ] **Step 1: Write the failing tests**

Open `tests/utils/test_loader.py` and add at the bottom:

```python
from utils.loader import MOTOR_COL_PATTERN

def test_motor_col_pattern_matches_velocity() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/Velocity")

def test_motor_col_pattern_velocity_captures_motor_id() -> None:
    m = MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-42/Velocity")
    assert m is not None
    assert m.group(1) == "42"

def test_motor_col_pattern_does_not_match_unknown_signal() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/Temperature") is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/y0shi/workspace/ignite/flashpoint
pytest tests/utils/test_loader.py::test_motor_col_pattern_matches_velocity tests/utils/test_loader.py::test_motor_col_pattern_velocity_captures_motor_id -v
```

Expected: FAIL — `Velocity` not in the pattern alternation.

- [ ] **Step 3: Update MOTOR_COL_PATTERN**

In `utils/loader.py`, change:

```python
MOTOR_COL_PATTERN = re.compile(
    r"Phoenix6/TalonFX-(\d+)/(MotorVoltage|StatorCurrent|SupplyVoltage|SupplyCurrent)"
)
```

to:

```python
MOTOR_COL_PATTERN = re.compile(
    r"Phoenix6/TalonFX-(\d+)/(MotorVoltage|StatorCurrent|SupplyVoltage|SupplyCurrent|Velocity)"
)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/utils/test_loader.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add utils/loader.py tests/utils/test_loader.py
git commit -m "feat: expand MOTOR_COL_PATTERN to capture TalonFX Velocity signal"
```

---

## Task 2: Add rotor_velocity field to MotorData

**Files:**
- Modify: `utils/models.py`
- Test: `tests/utils/test_models.py`

- [ ] **Step 1: Write the failing tests**

Open `tests/utils/test_models.py` and add at the bottom:

```python
def test_motor_data_rotor_velocity_defaults_to_none():
    n = 5
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
    )
    assert data.rotor_velocity is None


def test_motor_data_accepts_rotor_velocity():
    n = 5
    vel = np.linspace(0.0, 10.0, n)
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
        rotor_velocity=vel,
    )
    np.testing.assert_array_equal(data.rotor_velocity, vel)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/utils/test_models.py::test_motor_data_rotor_velocity_defaults_to_none tests/utils/test_models.py::test_motor_data_accepts_rotor_velocity -v
```

Expected: FAIL — `MotorData` has no `rotor_velocity` field.

- [ ] **Step 3: Add the field to MotorData**

In `utils/models.py`, after `supply_energy`:

```python
@dataclass
class MotorData:
    motor_voltage: np.ndarray
    stator_current: np.ndarray
    motor_power: np.ndarray
    motor_energy: np.ndarray

    supply_voltage: np.ndarray | None = None
    supply_current: np.ndarray | None = None
    supply_power: np.ndarray | None = None
    supply_energy: np.ndarray | None = None

    rotor_velocity: np.ndarray | None = None  # RPS
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/utils/test_models.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add utils/models.py tests/utils/test_models.py
git commit -m "feat: add rotor_velocity field to MotorData"
```

---

## Task 3: Wire velocity through the analyzer

**Files:**
- Modify: `utils/analyzer.py`
- Test: `tests/utils/test_analyzer.py`

- [ ] **Step 1: Write the failing tests**

Open `tests/utils/test_analyzer.py`. The existing `_make_df` helper at the top of the file needs a `with_velocity` variant. Add a helper and tests at the bottom:

```python
def _make_df_with_velocity() -> pd.DataFrame:
    data: dict = {
        "Timestamp": [0.0, 0.02, 0.04],
        "Phoenix6/TalonFX-1/MotorVoltage": [5.0, 6.0, 3.0],
        "Phoenix6/TalonFX-1/StatorCurrent": [2.0, 3.0, 1.0],
        "Phoenix6/TalonFX-1/SupplyVoltage": [12.0, 12.0, 12.0],
        "Phoenix6/TalonFX-1/SupplyCurrent": [3.0, 4.0, 2.0],
        "Phoenix6/TalonFX-1/Velocity": [5.0, 10.0, 7.5],
        "Phoenix6/TalonFX-2/MotorVoltage": [4.0, 5.0, 2.0],
        "Phoenix6/TalonFX-2/StatorCurrent": [1.5, 2.0, 0.5],
        "Phoenix6/TalonFX-2/SupplyVoltage": [12.0, 12.0, 12.0],
        "Phoenix6/TalonFX-2/SupplyCurrent": [2.0, 3.0, 1.0],
        "Phoenix6/TalonFX-2/Velocity": [3.0, 6.0, 4.5],
    }
    return pd.DataFrame(data)


def test_rotor_velocity_populated_when_column_present() -> None:
    motors = compute_motor_data(_make_df_with_velocity())
    assert motors["TalonFX-1"].rotor_velocity is not None
    np.testing.assert_array_almost_equal(
        motors["TalonFX-1"].rotor_velocity,
        [5.0, 10.0, 7.5],
    )


def test_rotor_velocity_none_when_column_absent() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    assert motors["TalonFX-1"].rotor_velocity is None


def test_compute_totals_leaves_rotor_velocity_none() -> None:
    motors = compute_motor_data(_make_df_with_velocity())
    timestamps = np.array([0.0, 0.02, 0.04])
    totals = compute_totals(motors, timestamps)
    assert totals.rotor_velocity is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/utils/test_analyzer.py::test_rotor_velocity_populated_when_column_present tests/utils/test_analyzer.py::test_rotor_velocity_none_when_column_absent tests/utils/test_analyzer.py::test_compute_totals_leaves_rotor_velocity_none -v
```

Expected: FAIL — `rotor_velocity` not yet set by `compute_motor_data`.

- [ ] **Step 3: Update compute_motor_data to capture Velocity**

In `utils/analyzer.py`, inside `compute_motor_data`, add velocity capture after the supply block. The full updated function body for each motor's block:

```python
    for motor_id in get_motor_ids(df):
        mv_col = _col(motor_id, "MotorVoltage")
        sc_col = _col(motor_id, "StatorCurrent")
        sv_col = _col(motor_id, "SupplyVoltage")
        su_col = _col(motor_id, "SupplyCurrent")
        vel_col = _col(motor_id, "Velocity")

        mv = df[mv_col].ffill().fillna(0.0).to_numpy() if mv_col in df.columns else zeros.copy()
        sc = df[sc_col].ffill().fillna(0.0).to_numpy() if sc_col in df.columns else zeros.copy()
        mp = mv * sc

        has_supply = sv_col in df.columns and su_col in df.columns
        has_velocity = vel_col in df.columns

        sv = df[sv_col].ffill().fillna(0.0).to_numpy() if has_supply else None
        su = df[su_col].ffill().fillna(0.0).to_numpy() if has_supply else None
        sp = sv * su if has_supply else None
        vel = df[vel_col].ffill().fillna(0.0).to_numpy() if has_velocity else None

        motors[motor_id] = MotorData(
            motor_voltage=mv,
            stator_current=sc,
            motor_power=mp,
            motor_energy=_cumulative_energy(mp, timestamps),
            supply_voltage=sv,
            supply_current=su,
            supply_power=sp,
            supply_energy=_cumulative_energy(sp, timestamps) if has_supply else None,
            rotor_velocity=vel,
        )
```

`compute_totals` requires no changes — `rotor_velocity` defaults to `None` on `MotorData`, so it's already omitted from totals.

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/utils/test_analyzer.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add utils/analyzer.py tests/utils/test_analyzer.py
git commit -m "feat: capture TalonFX Velocity signal in compute_motor_data"
```

---

## Task 4: Add plot_watts_per_rps to plotter

**Files:**
- Modify: `utils/plotter.py`
- Modify: `tests/utils/conftest.py`
- Test: `tests/utils/test_plotter.py`

- [ ] **Step 1: Add fixture to conftest**

Open `tests/utils/conftest.py`. Update `_make_motor_data` to accept a `with_velocity` flag, and add a new `simple_match_with_velocity` fixture.

Replace the existing `_make_motor_data` function:

```python
def _make_motor_data(n: int = 10, with_supply: bool = False, with_velocity: bool = False) -> MotorData:
    power = np.ones(n) * 100.0
    energy = np.cumsum(power * 0.02) / 3600
    kwargs: dict[str, Any] = dict(
        motor_voltage=np.ones(n) * 5.0,
        stator_current=np.ones(n) * 20.0,
        motor_power=power,
        motor_energy=energy,
    )
    if with_supply:
        kwargs.update(
            supply_voltage=np.ones(n) * 12.0,
            supply_current=np.ones(n) * 8.5,
            supply_power=np.ones(n) * 102.0,
            supply_energy=np.cumsum(np.ones(n) * 102.0 * 0.02) / 3600,
        )
    if with_velocity:
        kwargs["rotor_velocity"] = np.linspace(5.0, 15.0, n)
    return MotorData(**kwargs)
```

Then add a new fixture at the bottom of the file:

```python
@pytest.fixture
def simple_match_with_velocity() -> Match:
    n = 50  # enough samples that 3s smoothing window has data to work with
    t = np.arange(n) * 0.02
    data = _make_motor_data(n, with_supply=True, with_velocity=True)
    motors = {"TalonFX-1": data, "TalonFX-2": data}
    totals = MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=data.motor_power * 2,
        motor_energy=data.motor_energy * 2,
        supply_voltage=np.zeros(n),
        supply_current=np.zeros(n),
        supply_power=data.supply_power * 2,  # type: ignore[operator]
        supply_energy=data.supply_energy * 2,  # type: ignore[operator]
    )
    return Match(match_id="TEST_VEL", timestamps=t, motors=motors, totals=totals)
```

- [ ] **Step 2: Write the failing tests**

Open `tests/utils/test_plotter.py` and add at the bottom:

```python
def test_plot_watts_per_rps_returns_figure(simple_match_with_velocity) -> None:
    fig = plotter.plot_watts_per_rps(simple_match_with_velocity)
    assert isinstance(fig, Figure)


def test_plot_watts_per_rps_one_subplot_per_qualifying_motor(simple_match_with_velocity) -> None:
    # simple_match_with_velocity has 2 motors, both with supply + velocity
    fig = plotter.plot_watts_per_rps(simple_match_with_velocity)
    assert len(fig.axes) == 2


def test_plot_watts_per_rps_excludes_motors_without_supply(simple_match) -> None:
    # simple_match has no supply data — no motors qualify, returns empty figure
    fig = plotter.plot_watts_per_rps(simple_match)
    assert isinstance(fig, Figure)
    assert len(fig.axes) == 1  # empty placeholder axes


def test_plot_watts_per_rps_title_contains_match_id(simple_match_with_velocity) -> None:
    fig = plotter.plot_watts_per_rps(simple_match_with_velocity)
    assert "TEST_VEL" in fig.texts[0].get_text()


def test_plot_watts_per_rps_uses_motor_names(simple_match_with_velocity) -> None:
    motor_names = {"TalonFX-1": "FL Drive", "TalonFX-2": "FR Drive"}
    fig = plotter.plot_watts_per_rps(simple_match_with_velocity, motor_names=motor_names)
    titles = [ax.get_title() for ax in fig.axes]
    assert "FL Drive" in titles
    assert "FR Drive" in titles
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
pytest tests/utils/test_plotter.py::test_plot_watts_per_rps_returns_figure tests/utils/test_plotter.py::test_plot_watts_per_rps_one_subplot_per_qualifying_motor -v
```

Expected: FAIL — `plot_watts_per_rps` does not exist.

- [ ] **Step 4: Add constants and function to plotter.py**

In `utils/plotter.py`, add two constants after the existing `BIN_SECONDS` line:

```python
WATTS_PER_RPS_SMOOTH_S: float = 3.0
VEL_MIN_RPS: float = 1.0
```

Then add the new function at the end of the file:

```python
def plot_watts_per_rps(match: Match, motor_names: dict[str, str] | None = None) -> Figure:
    """Per-motor supply watts per RPS over time. Torque proxy; NaN gaps where velocity < VEL_MIN_RPS."""
    motor_ids = [
        mid for mid in _sorted_motor_ids(match, motor_names)
        if match.motors[mid].supply_power is not None
        and match.motors[mid].rotor_velocity is not None
    ]

    if not motor_ids:
        fig, ax = plt.subplots(figsize=(12, 3))
        ax.set_title(f"{match.match_id} — Supply Watts per RPS (no data)")
        ax.set_xlabel("Time (s)")
        ax.set_ylabel("W / RPS")
        fig.tight_layout()
        return fig

    fig, axes = plt.subplots(len(motor_ids), 1, figsize=(12, 3 * len(motor_ids)), sharex=True)
    if len(motor_ids) == 1:
        axes = [axes]

    for ax, motor_id in zip(axes, motor_ids):
        data = match.motors[motor_id]
        sp = _smooth_seconds(data.supply_power, match.timestamps, WATTS_PER_RPS_SMOOTH_S)  # type: ignore[arg-type]
        vel = _smooth_seconds(data.rotor_velocity, match.timestamps, WATTS_PER_RPS_SMOOTH_S)  # type: ignore[arg-type]
        vel_clamped = np.where(np.abs(vel) < VEL_MIN_RPS, np.nan, vel)
        ratio = sp / vel_clamped
        ax.plot(match.timestamps, ratio, linewidth=0.8)
        ax.set_ylabel("W / RPS")
        ax.set_title(_label(motor_id, motor_names))

    axes[-1].set_xlabel("Time (s)")
    fig.suptitle(f"{match.match_id} — Supply Watts per RPS (per motor)")
    fig.tight_layout()
    return fig
```

- [ ] **Step 5: Run all plotter tests**

```bash
pytest tests/utils/test_plotter.py -v
```

Expected: all PASS.

- [ ] **Step 6: Run the full test suite to check for regressions**

```bash
pytest tests/ -v
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add utils/plotter.py tests/utils/conftest.py tests/utils/test_plotter.py
git commit -m "feat: add plot_watts_per_rps — supply watts per RPS per motor"
```
