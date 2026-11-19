"""First-run smoke: install Flashpoint the documented way and take it through a team's first hour.

Usage:
    python tools/first-run-smoke.py                    # build the wheel from this checkout
    python tools/first-run-smoke.py --wheel dist/x.whl # test a built wheel (a release's)

Five steps, each named in the output, each with its own time limit (spec ci-quality-gates,
"First-run smoke on every platform"; design D-i of mvp-install-and-first-run):
  1. install  `uv tool install --python 3.13 <wheel>` into a fresh tool folder
  2. init     `flashpoint init --team 9999` with a fresh configuration, cache and lake
  3. ingest   fetch one corpus match, then `flashpoint ingest` (the real owlet download)
  4. replay   `flashpoint report`; the match must have aligned data
  5. serve    `flashpoint serve --port 0`; `/` and `/api/setup/checks` must answer

Everything runs from a folder outside the checkout, with FLASHPOINT_CONFIG, FLASHPOINT_CACHE and
the lake in fresh folders, so nothing of the developer's (or the runner's caches) is used.
Needs uv on PATH, and gh or a public corpus release for the corpus download. Standard library
only: it runs with whatever Python is on the runner.
"""

import argparse
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MANIFEST = REPO / "tests" / "corpus" / "manifest.toml"
FETCH_CORPUS = REPO / "tools" / "fetch-corpus.py"
DEFAULT_GROUP = "2026-gacmp-e10"
DEFAULT_MATCH = "2026gacmp_e10"
PYTHON = "3.13"  # the documented install's Python (ADR-0018)
TEAM = "9999"
STEPS = 5
LIMITS_S = {"install": 900, "init": 120, "ingest": 1200, "replay": 900}
STOP_GRACE_S = 10
URL = re.compile(r"http://127\.0\.0\.1:\d+/")
TAIL_LINES = 40


class StepError(RuntimeError):
    """A step failed; the message says why and carries the step's output."""


def _run(argv: Sequence[str], env: dict[str, str], cwd: Path, limit_s: float) -> str:
    try:
        done = subprocess.run(  # noqa: S603 - our own tools and the installed flashpoint
            list(argv), env=env, cwd=cwd, capture_output=True, text=True, timeout=limit_s,
            check=False,
        )  # fmt: skip
    except subprocess.TimeoutExpired as exc:
        out = (exc.stdout or b"").decode() if isinstance(exc.stdout, bytes) else exc.stdout or ""
        raise StepError(f"`{' '.join(argv)}` took over {limit_s:.0f} s\n{out}") from exc
    output = done.stdout + done.stderr
    if done.returncode != 0:
        raise StepError(f"`{' '.join(argv)}` exited {done.returncode}\n{output}")
    return output


def _tail(text: str) -> str:
    return "\n".join(text.strip().splitlines()[-TAIL_LINES:])


class Smoke:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.work = Path(args.work or tempfile.mkdtemp(prefix="flashpoint-smoke-")).absolute()
        self.work.mkdir(parents=True, exist_ok=True)
        self.lake = self.work / "lake"
        self.bin = self.work / "bin"
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("FLASHPOINT_")}
        self.env |= {
            "FLASHPOINT_CONFIG": str(self.work / "config"),
            "FLASHPOINT_CACHE": str(self.work / "cache"),  # no owlet cache: the real download
            "UV_TOOL_DIR": str(self.work / "uv-tools"),
            "UV_TOOL_BIN_DIR": str(self.bin),
            "PYTHONUTF8": "1",
        }
        if "FLASHPOINT_CORPUS_DIR" in os.environ:  # the corpus itself may come from a cache
            self.env["FLASHPOINT_CORPUS_DIR"] = os.environ["FLASHPOINT_CORPUS_DIR"]

    @property
    def flashpoint(self) -> str:
        name = "flashpoint.exe" if sys.platform == "win32" else "flashpoint"
        return str(self.bin / name)

    def fp(self, *args: str, limit: str) -> str:
        return _run([self.flashpoint, *args], self.env, self.work, LIMITS_S[limit])

    # --- steps ------------------------------------------------------------------------

    def install(self) -> str:
        uv = shutil.which("uv")
        if uv is None:
            raise StepError("uv is not on PATH (install it: https://docs.astral.sh/uv/)")
        wheel = Path(self.args.wheel).absolute() if self.args.wheel else self._build(uv)
        output = _run([uv, "tool", "install", "--python", PYTHON, str(wheel)], self.env,
                      self.work, LIMITS_S["install"])  # fmt: skip
        version = self.fp("--version", limit="init").strip()
        expected = wheel.name.split("-")[1]
        if version != f"flashpoint {expected}":
            raise StepError(f"--version says {version!r}, the wheel is {expected}")
        return f"{output}\n{version}"

    def _build(self, uv: str) -> Path:
        dist = self.work / "dist"
        _run([uv, "build", "--wheel", "--out-dir", str(dist), str(REPO)], self.env, self.work,
             LIMITS_S["install"])  # fmt: skip
        (wheel,) = dist.glob("*.whl")
        return wheel

    def init(self) -> str:
        return self.fp("init", "--team", TEAM, "--lake", str(self.lake), "--team-name", "",
                       "--palette", "amber", limit="init")  # fmt: skip

    def ingest(self) -> str:
        fetched = _run([sys.executable, str(FETCH_CORPUS), "--group", self.args.group], self.env,
                       self.work, LIMITS_S["ingest"])  # fmt: skip
        manifest = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))
        root = Path(self.env.get("FLASHPOINT_CORPUS_DIR", Path.home() / ".cache/flashpoint/corpus"))
        files = [str(root / manifest["version"] / e["name"]) for e in manifest["file"]
                 if e["group"] == self.args.group]  # fmt: skip
        return fetched + self.fp("ingest", "--lake", str(self.lake), *files, limit="ingest")

    def replay(self) -> str:
        output = self.fp("report", "--lake", str(self.lake), limit="replay")
        index_path = self.lake / "report" / "data" / "matches.js"
        text = index_path.read_text(encoding="utf-8")
        index = json.loads(text[text.index("(") + 1 : text.rindex(")")])
        entries = {m["key"]: m for m in index["matches"]}
        match = self.args.match
        if match not in entries:
            raise StepError(f"{match} is not in Replay ({sorted(entries)})\n{output}")
        status = entries[match]["status"]
        if status == "no-aligned-samples":
            reason = entries[match].get("reason", "")
            raise StepError(f"{match} has status {status}: {reason}\n{output}")
        return f"{output}\n{match}: {status}"

    def serve(self) -> str:
        options: dict[str, object] = {}
        if sys.platform == "win32":
            options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # for Ctrl+Break
        server = subprocess.Popen(  # noqa: S603 - the installed flashpoint
            [self.flashpoint, "serve", "--lake", str(self.lake), "--port", "0", "--no-build"],
            env=self.env, cwd=self.work, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, **options,  # type: ignore[call-overload]  # platform-only creationflags
        )  # fmt: skip
        print(f"server pid {server.pid}", flush=True)
        lines: list[str] = []
        reader = threading.Thread(target=lambda: lines.extend(server.stdout or []), daemon=True)
        reader.start()
        try:
            return self._answers(server, lines)
        finally:
            self._stop(server)
            reader.join(timeout=STOP_GRACE_S)

    def _answers(self, server: "subprocess.Popen[str]", lines: list[str]) -> str:
        deadline = time.monotonic() + self.args.serve_timeout
        url = None
        while url is None and time.monotonic() < deadline and server.poll() is None:
            url = next((m[0] for line in list(lines) if (m := URL.search(line))), None)
            time.sleep(0.2)
        if url is None:
            raise StepError(f"serve printed no address\n{''.join(lines)}")
        if self.args.inject_serve_failure:
            url = f"http://127.0.0.1:{_free_port()}/"  # nothing listens there
        for path in ("", "api/setup/checks"):
            self._get(url + path, deadline, lines)
        return "".join(lines) + f"\n{url} and {url}api/setup/checks answered"

    def _get(self, url: str, deadline: float, lines: list[str]) -> None:
        last = "no attempt"
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310 - localhost
                    if response.status == 200:
                        return
                    last = f"HTTP {response.status}"
            except (urllib.error.URLError, OSError) as exc:
                last = str(exc)
            time.sleep(0.5)
        raise StepError(f"{url} didn't answer within {self.args.serve_timeout} s ({last})\n"
                        + "".join(lines))  # fmt: skip

    @staticmethod
    def _stop(server: "subprocess.Popen[str]") -> None:
        if server.poll() is not None:
            return
        if sys.platform == "win32":
            server.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]  # Windows only
        else:
            server.send_signal(signal.SIGINT)
        try:
            server.wait(timeout=STOP_GRACE_S)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=STOP_GRACE_S)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _peak_child_rss_mb() -> float | None:
    try:
        import resource  # not on Windows
    except ImportError:
        return None
    peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024  # bytes vs KiB


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--wheel", help="install this wheel instead of building one")
    parser.add_argument("--work", help="working folder (default: a new temporary folder)")
    parser.add_argument("--group", default=DEFAULT_GROUP, help="corpus group to ingest")
    parser.add_argument("--match", default=DEFAULT_MATCH, help="match key that must have data")
    parser.add_argument("--serve-timeout", type=float, default=60.0, help="seconds for serve")
    parser.add_argument("--inject-serve-failure", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    smoke = Smoke(args)
    print(f"first-run smoke in {smoke.work}", flush=True)
    started = time.monotonic()
    for number, name in enumerate(("install", "init", "ingest", "replay", "serve"), start=1):
        print(f"== step {number}/{STEPS}: {name} ==", flush=True)
        step_started = time.monotonic()
        try:
            output = getattr(smoke, name)()
        except StepError as exc:
            print(f"FAILED {name}: {_tail(str(exc))}", flush=True)
            return 1
        print(_tail(output), flush=True)
        print(f"ok     {name} ({time.monotonic() - step_started:.1f} s)", flush=True)
    peak = _peak_child_rss_mb()
    shown = f", peak child RSS {peak:.0f} MB" if peak is not None else ""
    print(f"first-run smoke passed in {time.monotonic() - started:.0f} s{shown}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
