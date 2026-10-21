"""A copy of `config/` whose brownout and sag thresholds sit above Q7's lowest battery proxy
voltage (the gyro bottoms out near 8.1 V), so corpus scenarios that need a brownout have one.
With the shipped thresholds the corpus has no brownout at all."""

import re
import shutil
from pathlib import Path

from flashpoint import config

BROWNOUT_V = 8.5
SAG_V = 9.0
# Q7's brownout count under BROWNOUT_V: a change means the proxy or its rule changed.
Q7_BROWNOUTS = 3  # gyro lows 8.10, 8.27, 8.22 V (battery-proxy 4.1)


def write_brownout_config(target: Path) -> Path:
    """Copy the repo configuration to `target` with the test thresholds; returns `target`."""
    shutil.copytree(config.config_root(), target)
    report = target / "report.toml"
    text = config.config_file(target, "report.toml").read_text(encoding="utf-8")
    text = re.sub(r"(?m)^brownout_v = [0-9.]+", f"brownout_v = {BROWNOUT_V}", text)
    text = re.sub(r"(?m)^sag_v = [0-9.]+", f"sag_v = {SAG_V}", text)
    report.write_text(text, encoding="utf-8")
    return target
