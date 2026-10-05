import xml.etree.ElementTree as ET
from pathlib import Path

DEPLOY = Path(__file__).resolve().parents[1] / "deploy"
NS = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}


def _text(root: ET.Element, path: str) -> str | None:
    return root.findtext(path, namespaces=NS)


def test_windows_task_runs_watch_at_logon() -> None:
    root = ET.parse(DEPLOY / "windows" / "flashpoint-acquire.xml").getroot()
    assert root.find("t:Triggers/t:LogonTrigger", NS) is not None
    assert _text(root, "t:Actions/t:Exec/t:Command") == r"%USERPROFILE%\.local\bin\flashpoint.exe"
    assert _text(root, "t:Actions/t:Exec/t:Arguments") == "acquire --watch"
    assert _text(root, "t:Settings/t:RestartOnFailure/t:Count") == "3"
    assert _text(root, "t:Settings/t:ExecutionTimeLimit") == "PT0S"
    assert _text(root, "t:Settings/t:MultipleInstancesPolicy") == "IgnoreNew"


def test_systemd_unit_is_single_absolute_exec() -> None:
    lines = (DEPLOY / "systemd" / "flashpoint-acquire.service").read_text().splitlines()
    entries = dict(line.split("=", 1) for line in lines if "=" in line and line[0] != "#")
    assert entries["ExecStart"] == "%h/.local/bin/flashpoint acquire --watch"
    assert entries["Restart"] == "on-failure"
    assert entries["WantedBy"] == "default.target"
