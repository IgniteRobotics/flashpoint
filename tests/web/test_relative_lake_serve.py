"""`flashpoint serve --lake lake` (relative) serves the anomalies API (#107).

Spec: telemetry-lake / Lake location and portability (Server with a relative lake).
"""

from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.views.anomalies import AnomalyQueries
from tests.web.anomaly_lake import build_anomaly_lake


def test_anomalies_api_with_a_relative_lake(
    serve: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    built, config_dir = build_anomaly_lake(tmp_path)  # the lake is tmp_path/lake
    Ledger(built.ledger).close()  # an ingested lake has one; the triage read opens it by URI
    monkeypatch.chdir(tmp_path)
    lake = LakePaths(config.lake_root(Path("lake")))  # as `serve --lake lake` builds it
    assert lake.ledger.is_file()
    app = serve(lake, anomalies=AnomalyQueries(lake, config_dir))

    status, body = app.json("/api/anomalies")
    assert status == 200, body
    assert body["counts"]["open"] == 2
    assert app.json("/api/anomalies/summary")[0] == 200
