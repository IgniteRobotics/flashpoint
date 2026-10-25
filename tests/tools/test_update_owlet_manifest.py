import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "update-owlet-manifest.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("update_owlet_manifest", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


update_owlet_manifest = _load()


def test_help_exits_zero() -> None:
    with pytest.raises(SystemExit) as exc:
        update_owlet_manifest.main(["--help"])
    assert exc.value.code == 0


def test_unknown_argument_exits_two() -> None:
    with pytest.raises(SystemExit) as exc:
        update_owlet_manifest.main(["--nothelp"])
    assert exc.value.code == 2
