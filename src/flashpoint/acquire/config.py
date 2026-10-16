"""Acquisition settings: `<config_root>/acquire.toml` with CLI overrides on top."""

import tomllib
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, ValidationError

CONFIG_FILE = "acquire.toml"


class AcquireConfigError(ValueError):
    """The configuration file or an override is invalid; the message names the bad key."""


class BackupConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    remote: str | None = None
    interval_min: int = 15


class AcquireConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    hosts: list[str] = ["10.68.29.2", "roborio-6829-frc.local", "172.22.11.2"]
    user: str = "lvuser"
    password: str = ""
    roots: list[str] = ["/home/lvuser/logs", "/u/logs"]
    poll_s: int = 30
    settle_s: int = 5
    low_space_mb: int = 100
    removable_media: bool = True
    backup: BackupConfig = BackupConfig()

    @classmethod
    def load(cls, config_root: Path) -> Self:
        """Read `acquire.toml` from the config root; no file means all defaults."""
        path = config_root / CONFIG_FILE
        if not path.is_file():
            return cls()
        try:
            data = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise AcquireConfigError(f"{path}: {exc}") from exc
        return cls._validate(data, str(path))

    def with_overrides(self, **overrides: Any) -> Self:
        """Return a copy with every override that is not None applied over this config."""
        merged = self.model_dump() | {k: v for k, v in overrides.items() if v is not None}
        return self._validate(merged, "command line")

    @classmethod
    def _validate(cls, data: dict[str, Any], origin: str) -> Self:
        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            problems = "; ".join(
                f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
            )
            raise AcquireConfigError(f"{origin}: {problems}") from exc
