"""On-disk layout of a Flashpoint lake."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LakePaths:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze" / "samples"

    @property
    def staging(self) -> Path:
        return self.root / "bronze" / "_staging"

    @property
    def meta(self) -> Path:
        return self.root / "meta"

    @property
    def ledger(self) -> Path:
        return self.meta / "flashpoint.sqlite"

    @property
    def inbox(self) -> Path:
        return self.root / "inbox"

    @property
    def tmp(self) -> Path:
        return self.root / "tmp"

    @property
    def status(self) -> Path:
        return self.meta / "acquire-status.json"

    def ensure(self) -> None:
        for path in (self.raw, self.bronze, self.staging, self.meta):
            path.mkdir(parents=True, exist_ok=True)
