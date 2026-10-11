"""Detection settings from `config/anomaly.toml`: SPEC limits, per-unit baselines, sibling
comparison, strict-sensitivity thresholds, and the optional model detector."""

import hashlib
import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

FILE_NAME = "anomaly.toml"
# Gold columns judged against each unit's own history (match_features, then can_health).
BASELINE_METRICS = (
    "temp_max_c",
    "temp_mean_c",
    "temp_rise_c_per_min",
    "stator_current_p95",
    "residual_p95_a",
    "stall_s",
    "isolated_gaps",
    "missed_per_min",
)
# Metrics only SPEC limits judge; `brownouts` belongs to a robot's `power` subject.
SPEC_ONLY_METRICS = ("reboots", "longest_gap_ms", "brownouts")
MODEL_METRIC = "feature_vector"
METRICS = (*BASELINE_METRICS, *SPEC_ONLY_METRICS, MODEL_METRIC)
METHODS = ("SPEC", "DRIFT", "STEP", "SIBLING", "MODEL")
# metric -> (display name, unit)
METRIC_INFO = {
    "temp_max_c": ("maximum temperature", "°C"),
    "temp_mean_c": ("mean temperature", "°C"),
    "temp_rise_c_per_min": ("temperature rise rate", "°C/min"),
    "stator_current_p95": ("stator current P95", "A"),
    "residual_p95_a": ("current residual P95", "A"),
    "stall_s": ("stall time", "s"),
    "isolated_gaps": ("isolated CAN gaps", "gaps"),
    "missed_per_min": ("CAN frames missed per enabled minute", "/min"),
    "reboots": ("reboots", "reboots"),
    "longest_gap_ms": ("longest isolated CAN gap", "ms"),
    "brownouts": ("brownouts", "brownouts"),
    MODEL_METRIC: ("feature vector", ""),
}
POWER = "power"  # the subject (and label slot) of a robot's brownouts
# A spread floor per metric, so a perfectly steady history cannot make every change infinite.
DEFAULT_FLOORS = {
    "temp_max_c": 1.0,
    "temp_mean_c": 1.0,
    "temp_rise_c_per_min": 0.5,
    "stator_current_p95": 2.0,
    "residual_p95_a": 5.0,  # the residual is noisy (one Q76 drive row: teleop P95 115 A)
    "stall_s": 0.5,
    "isolated_gaps": 1.0,
    "missed_per_min": 0.5,
}


class AnomalyConfigError(Exception):
    pass


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SpecLimits(_Section):
    temp_max_med_c: float = Field(default=65.0, ge=0)
    temp_max_high_c: float = Field(default=75.0, ge=0)
    temp_mean_med_c: float = Field(default=55.0, ge=0)
    stall_med_s: float = Field(default=3.0, ge=0)
    gap_med_ms: float = Field(default=250.0, ge=0)  # longest isolated CAN gap
    gap_high_ms: float = Field(default=1000.0, ge=0)
    brownout_high: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def _ordered(self) -> "SpecLimits":
        if self.temp_max_high_c < self.temp_max_med_c:
            raise ValueError("temp_max_high_c must not be below temp_max_med_c")
        if self.gap_high_ms < self.gap_med_ms:
            raise ValueError("gap_high_ms must not be below gap_med_ms")
        return self


class Baseline(_Section):
    metrics: list[str] = Field(default_factory=lambda: list(BASELINE_METRICS))
    window: int = Field(default=20, ge=1)
    min_baseline: int = Field(default=5, ge=1)
    step_z: float = Field(default=3.5, gt=0)
    step_persist: int = Field(default=3, ge=1)
    drift_z: float = Field(default=2.5, gt=0)
    drift_span: int = Field(default=5, ge=1)
    med_z: float = Field(default=4.0, gt=0)  # DRIFT, STEP and SIBLING severity: MED at |z| >= med_z
    high_z: float = Field(default=6.0, gt=0)
    floors: dict[str, float] = Field(default_factory=lambda: dict(DEFAULT_FLOORS))

    @field_validator("metrics")
    @classmethod
    def _metrics(cls, value: list[str]) -> list[str]:
        unknown = sorted(set(value) - set(BASELINE_METRICS))
        if unknown:
            raise ValueError(f"unknown metrics {unknown}; known: {list(BASELINE_METRICS)}")
        return value

    @field_validator("floors", mode="before")
    @classmethod
    def _floors(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        unknown = sorted(set(value) - set(BASELINE_METRICS))
        if unknown:
            raise ValueError(f"unknown metrics {unknown} in floors")
        negative = sorted(k for k, v in value.items() if isinstance(v, int | float) and v <= 0)
        if negative:
            raise ValueError(f"floors {negative} must be positive")
        return {**DEFAULT_FLOORS, **value}

    @model_validator(mode="after")
    def _ordered(self) -> "Baseline":
        if self.high_z < self.med_z:
            raise ValueError("high_z must not be below med_z")
        return self


class Sibling(_Section):
    z: float = Field(default=3.0, gt=0)
    need: int = Field(default=3, ge=1)  # flagged in at least `need` ...
    of: int = Field(default=5, ge=1)  # ... of the last `of` matches the whole group played

    @model_validator(mode="after")
    def _ordered(self) -> "Sibling":
        if self.need > self.of:
            raise ValueError("need must not exceed of")
        return self


class Strict(_Section):
    step_z: float = Field(default=2.5, gt=0)
    drift_z: float = Field(default=1.5, gt=0)
    sibling_z: float = Field(default=2.0, gt=0)
    persist: int = Field(default=2, ge=1)  # STEP persistence and SIBLING `need`


class Model(_Section):
    enabled: bool = False
    contamination: float = Field(default=0.01, gt=0, lt=0.5)
    min_train: int = Field(default=200, ge=1)


class AnomalyConfig(_Section):
    spec: SpecLimits = SpecLimits()
    baseline: Baseline = Baseline()
    sibling: Sibling = Sibling()
    strict: Strict = Strict()
    model: Model = Model()

    @model_validator(mode="after")
    def _strict_not_above_normal(self) -> "AnomalyConfig":
        pairs = (
            ("step_z", self.strict.step_z, self.baseline.step_z),
            ("drift_z", self.strict.drift_z, self.baseline.drift_z),
            ("sibling_z", self.strict.sibling_z, self.sibling.z),
            ("persist", self.strict.persist, self.baseline.step_persist),
        )
        for key, strict, normal in pairs:
            if strict > normal:
                raise ValueError(f"strict.{key} ({strict}) is above its normal value ({normal})")
        if self.strict.persist > self.sibling.need:
            raise ValueError(
                f"strict.persist ({self.strict.persist}) is above"
                f" sibling.need ({self.sibling.need})"
            )
        return self

    def digest(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


def load_anomaly_config(config_dir: Path) -> AnomalyConfig:
    """Defaults when the file is absent; unknown keys and bad values name the file and key."""
    path = config_dir / FILE_NAME
    if not path.is_file():
        return AnomalyConfig()
    try:
        with path.open("rb") as f:
            return AnomalyConfig.model_validate(tomllib.load(f))
    except (tomllib.TOMLDecodeError, ValidationError) as exc:
        raise AnomalyConfigError(f"{path.name}: {exc}") from exc
