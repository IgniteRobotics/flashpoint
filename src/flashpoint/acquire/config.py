"""Acquisition settings: `<config_root>/acquire.toml` with CLI overrides on top."""

import tomllib
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError

CONFIG_FILE = "acquire.toml"
USB_HOST = "172.22.11.2"


def team_hosts(team: int) -> list[str]:
    """The roboRIO addresses for a team: 10.TE.AM.2, its mDNS name, then USB."""
    return [f"10.{team // 100}.{team % 100}.2", f"roborio-{team}-frc.local", USB_HOST]


class AcquireConfigError(ValueError):
    """The configuration file or an override is invalid; the message names the bad key."""


class BackupConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    remote: str | None = None
    interval_min: int = 15
    # Logs or configuration waiting longer than this make the backup overdue (backup-status).
    overdue_h: int = Field(default=24, ge=1)


class AcquireConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    team: int | None = Field(default=None, ge=1, le=99999, strict=True)
    lake: Path | None = None
    hosts: list[str] = []
    user: str = "lvuser"
    password: str = ""
    roots: list[str] = ["/home/lvuser/logs", "/u/logs"]
    poll_s: int = 30
    settle_s: int = 5
    low_space_mb: int = 100
    removable_media: bool = True
    backup: BackupConfig = BackupConfig()

    def robot_hosts(self) -> list[str]:
        """Configured hosts, else the team's addresses, else none (acquire refuses)."""
        if self.hosts:
            return list(self.hosts)
        return team_hosts(self.team) if self.team is not None else []

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
