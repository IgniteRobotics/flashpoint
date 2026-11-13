import pytest


def test_parse() -> None:
    with pytest.raises(ValueError):
        int("x")
