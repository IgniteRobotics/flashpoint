import flashpoint


def test_package_exposes_version() -> None:
    assert flashpoint.__version__ == "0.1.0"


def test_cli_main_returns_zero() -> None:
    from flashpoint.cli import main

    assert main([]) == 0
