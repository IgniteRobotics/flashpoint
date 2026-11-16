import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "update-owlet-manifest.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("update_owlet_manifest", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


update_owlet_manifest = _load()


@pytest.fixture(autouse=True)
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("update-owlet-manifest made a network request")

    monkeypatch.setattr(update_owlet_manifest.urllib.request, "urlopen", refuse)


def test_help_exits_zero_without_a_network_request(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        update_owlet_manifest.main(["--help"])
    assert exc.value.code == 0
    assert "usage:" in capsys.readouterr().out


def test_unknown_argument_exits_two_without_a_network_request() -> None:
    with pytest.raises(SystemExit) as exc:
        update_owlet_manifest.main(["--nothelp"])
    assert exc.value.code == 2
