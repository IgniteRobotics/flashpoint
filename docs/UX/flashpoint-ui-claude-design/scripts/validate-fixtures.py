#!/usr/bin/env python3
"""Validate data-contracts/examples against their JSON Schemas.  pip install jsonschema"""
import json, pathlib, sys
from jsonschema import Draft202012Validator
root = pathlib.Path(__file__).resolve().parent.parent / "data-contracts"
pairs = [("live-snapshot.json","live-snapshot"),("match-log-q42.json","match-log"),("pit-check.json","pit-check"),
         ("devices.json","device[]"),("device-metrics-m4.json","device-metrics"),("anomalies.json","anomaly[]")]
ok = True
for ex, sc in pairs:
    many = sc.endswith("[]"); sc = sc.rstrip("[]")
    v = Draft202012Validator(json.loads((root / f"{sc}.schema.json").read_text()))
    data = json.loads((root / "examples" / ex).read_text())
    errs = [e for item in (data if many else [data]) for e in v.iter_errors(item)]
    print(("PASS " if not errs else "FAIL ") + ex + (f" ({len(data)} items)" if many else ""))
    for e in errs[:5]: print("   ", list(e.path), e.message)
    ok &= not errs
sys.exit(0 if ok else 1)
