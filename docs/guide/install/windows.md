# Install on Windows

You need Windows on an x86-64 PC; Windows on ARM isn't supported yet. Flashpoint brings its own Python, so you don't need to install one. Run these commands in PowerShell.

## Install

1. Install [uv](https://docs.astral.sh/uv/), once:

    ```powershell
    winget install --id=astral-sh.uv -e
    ```

    Without winget, use uv's installer: `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`. Open a new PowerShell window afterwards.

2. Install Flashpoint:

    ```powershell
    uv tool install --python 3.13 flashpoint
    ```

    uv downloads Python 3.13 for Flashpoint if your PC doesn't have it. Nothing else changes.

3. Create your configuration:

    ```powershell
    flashpoint init --team <your team number>
    ```

    `init` asks for anything you leave out: your lake folder (where logs and results are stored, `%USERPROFILE%\flashpoint-lake` by default), your team name and the app's colours. It writes your config folder, `%APPDATA%\flashpoint`. See [`flashpoint init`](../reference/cli.md#flashpoint-init).

## Check it worked

```powershell
flashpoint --version
flashpoint doctor
```

`--version` prints `flashpoint` and a version number. `doctor` lists the setup checks: right after `init`, the robot check is a warning until you import this season's logs, which is expected.

If PowerShell says `flashpoint` isn't recognized, run `uv tool update-shell` and open a new window.

## With pipx instead

pipx needs a Python 3.11 to 3.14 already installed. Windows has no `python3.13` command, so use the `py` launcher: in the [python.org](https://www.python.org/downloads/windows/) installer for Python 3.13, tick "py launcher". Then:

```powershell
py -3.13 -m pip install --user pipx
py -3.13 -m pipx install flashpoint
```

## Before your first event

Flashpoint converts hoot files with CTRE's owlet tool, which it downloads the first time it sees a hoot of each format. Do that while you still have internet: ingest one hoot from this season's robot code, then check that `doctor` says owlet is `downloaded, verified`.

```powershell
flashpoint ingest <a recent .hoot file>
flashpoint doctor
```

For a laptop that never goes online, see [Install without internet](../how-to/install-offline.md).

## Updating

```powershell
uv tool upgrade flashpoint
```

With pipx, `py -3.13 -m pipx upgrade flashpoint`. Your config folder and lake are kept either way. To move from pipx to uv, run `py -3.13 -m pipx uninstall flashpoint`, then the uv install above; your config folder and lake aren't touched.

Next: [Quickstart](../quickstart.md).
