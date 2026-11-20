"""Report the nightly first-run smoke's pass rate per OS (mvp-install-and-first-run task 9.5).

Usage:
    python tools/smoke-pass-rate.py              # the last 7 days, Markdown on stdout
    python tools/smoke-pass-rate.py --days 14

Reads the scheduled runs of `.github/workflows/first-run-smoke.yml` through `gh` (log in with
`gh auth login`, or set GH_TOKEN). A cancelled or skipped job isn't a verdict and isn't counted.
Standard library only.
"""

import argparse
import datetime as dt
import json
import re
import shutil
import subprocess
import sys
from collections.abc import Sequence
from typing import Any

WORKFLOW = "first-run-smoke.yml"
OSES = ("ubuntu-latest", "macos-latest", "windows-latest")
VERDICTS = {"success", "failure", "timed_out"}
JOB = re.compile(r"\((?P<os>[a-z]+-latest)\)")
RUN_LIMIT = 200


def os_of(job_name: str) -> str | None:
    match = JOB.search(job_name)
    return match["os"] if match else None


def summarize(runs: list[dict[str, Any]], days: int, now: dt.datetime) -> str:
    """Markdown: one row per OS (passed of judged runs) and a line per failure."""
    since = now - dt.timedelta(days=days)
    shown = sorted((r for r in runs if r["created"] >= since), key=lambda r: r["created"])
    title = f"First-run smoke, nightly runs over the last {days} days"
    if not shown:
        return f"### {title}\n\nNo nightly runs in that window.\n"
    first, last = shown[0]["created"].date(), shown[-1]["created"].date()
    count = f"{len(shown)} nightly run{'s' if len(shown) != 1 else ''}"
    lines = [f"### {title}", "", f"{count}, {first} to {last}.", "",
             "| OS | passed | pass rate |", "|---|---|---|"]  # fmt: skip
    failures = []
    for os_name in OSES:
        judged = [r for r in shown if r["jobs"].get(os_name) in VERDICTS]
        passed = sum(1 for r in judged if r["jobs"][os_name] == "success")
        pct = f"{round(100 * passed / len(judged))}%" if judged else "n/a"
        lines.append(f"| {os_name} | {passed} of {len(judged)} | {pct} |")
        failures += [
            (r["created"], os_name, r["jobs"][os_name], r["url"])
            for r in judged
            if r["jobs"][os_name] != "success"
        ]
    if failures:
        lines += ["", "Failures:"]
        lines += [f"- {when.date()} {os_name}: {verdict} ({url})"
                  for when, os_name, verdict, url in sorted(failures)]  # fmt: skip
    return "\n".join(lines) + "\n"


def _gh(gh: str, *args: str) -> Any:
    done = subprocess.run([gh, *args], capture_output=True, text=True, check=True)  # noqa: S603 - gh from PATH with our own arguments
    return json.loads(done.stdout)


def fetch(gh: str) -> list[dict[str, Any]]:
    listed = _gh(gh, "run", "list", "--workflow", WORKFLOW, "--event", "schedule",
                 "--limit", str(RUN_LIMIT), "--json", "databaseId,createdAt,url")  # fmt: skip
    runs = []
    for run in listed:
        view = _gh(gh, "run", "view", str(run["databaseId"]), "--json", "jobs")
        jobs = {os_of(j["name"]): j["conclusion"] or j["status"] for j in view["jobs"]}
        runs.append({
            "created": dt.datetime.fromisoformat(run["createdAt"].replace("Z", "+00:00")),
            "url": run["url"],
            "jobs": {k: v for k, v in jobs.items() if k},
        })  # fmt: skip
    return runs


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--days", type=int, default=7, help="how many days back (default 7)")
    args = parser.parse_args(argv)
    gh = shutil.which("gh")
    if gh is None:
        print("gh is not on PATH: https://cli.github.com/", file=sys.stderr)
        return 2
    print(summarize(fetch(gh), args.days, dt.datetime.now(dt.UTC)), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
