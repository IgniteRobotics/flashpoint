# Install without internet

Prepare one folder on a computer with internet, carry it on a USB stick, and install Flashpoint from it on a pit laptop that never goes online. The folder holds everything the install and the first hoot import would otherwise download: Flashpoint and its dependencies, a Python, uv, the owlet hoot converter, and these docs.

## Before you start

- An online computer with the **same operating system and processor** as the pit laptop: an Apple silicon or Intel Mac, a Linux x86-64 PC, or a Windows x86-64 PC. On macOS and Linux, `uname -m` prints `arm64` or `x86_64`. Python and uv are built for one system, so a folder made on a Mac won't install on Windows.
- uv on the online computer ([Install](../install/index.md)), and Flashpoint installed there too, to download owlet.
- One hoot from each season your logs come from, such as last season's and this season's robot code. Each season's Phoenix release can use a new hoot format, and each format needs its own owlet.
- A USB stick with about 1 GB free. The folder is about 300 MB.

This page uses Python 3.13, the version the normal install uses.

## Collect the folder (on the online computer)

Every command runs from the folder that will hold `flashpoint-offline`, such as your home folder. On Windows, run them in PowerShell.

1. Make the folder:

    ```sh
    mkdir -p flashpoint-offline/wheels
    ```

    ```powershell
    New-Item -ItemType Directory -Force flashpoint-offline\wheels
    ```

2. Download Flashpoint and every dependency it needs, as wheels for the pit laptop's system and Python 3.13. Use the line for the pit laptop:

    macOS, Apple silicon:

    ```sh
    uvx --python 3.13 pip download --only-binary=:all: --python-version 3.13 --platform macosx_13_0_arm64 flashpoint -d flashpoint-offline/wheels
    ```

    macOS, Intel:

    ```sh
    uvx --python 3.13 pip download --only-binary=:all: --python-version 3.13 --platform macosx_13_0_x86_64 flashpoint -d flashpoint-offline/wheels
    ```

    Linux x86-64 (glibc 2.35 or later; pip needs each tag listed):

    ```sh
    uvx --python 3.13 pip download --only-binary=:all: --python-version 3.13 --platform manylinux_2_35_x86_64 --platform manylinux_2_34_x86_64 --platform manylinux_2_28_x86_64 --platform manylinux_2_17_x86_64 --platform manylinux2014_x86_64 flashpoint -d flashpoint-offline/wheels
    ```

    Windows x86-64:

    ```powershell
    uvx --python 3.13 pip download --only-binary=:all: --python-version 3.13 --platform win_amd64 flashpoint -d flashpoint-offline\wheels
    ```

    It ends with `Successfully downloaded flashpoint` and the names of the other packages. Every dependency has wheels for these systems, so nothing needs building. For Flashpoint 0.2.0 that's 27 files.

3. Download Python 3.13 for the pit laptop, and pack uv's copy of it:

    ```sh
    uv python install 3.13
    (cd "$(uv python dir)" && tar -czf "$OLDPWD/flashpoint-offline/python.tar.gz" cpython-3.13.*)
    ```

    ```powershell
    uv python install 3.13
    Compress-Archive -Path "$(uv python dir)\cpython-3.13.*" -DestinationPath flashpoint-offline\python.zip
    ```

4. Download uv for the pit laptop. Use the line for the pit laptop:

    ```sh
    # macOS, Apple silicon
    curl -L -o flashpoint-offline/uv.tar.gz https://github.com/astral-sh/uv/releases/latest/download/uv-aarch64-apple-darwin.tar.gz
    # macOS, Intel
    curl -L -o flashpoint-offline/uv.tar.gz https://github.com/astral-sh/uv/releases/latest/download/uv-x86_64-apple-darwin.tar.gz
    # Linux x86-64
    curl -L -o flashpoint-offline/uv.tar.gz https://github.com/astral-sh/uv/releases/latest/download/uv-x86_64-unknown-linux-gnu.tar.gz
    ```

    ```powershell
    Invoke-WebRequest -Uri https://github.com/astral-sh/uv/releases/latest/download/uv-x86_64-pc-windows-msvc.zip -OutFile flashpoint-offline\uv.zip
    ```

5. Download owlet for each hoot format your logs use, by importing one hoot of each season into a scratch lake. Then pack the owlet cache:

    ```sh
    flashpoint ingest --lake <scratch folder> <last season's .hoot> <this season's .hoot>
    tar -czf flashpoint-offline/owlet.tar.gz -C ~/.cache/flashpoint owlet
    ```

    ```powershell
    flashpoint ingest --lake <scratch folder> <last season's .hoot> <this season's .hoot>
    Compress-Archive -Path "$HOME\.cache\flashpoint\owlet" -DestinationPath flashpoint-offline\owlet.zip
    ```

    If you set `FLASHPOINT_CACHE`, pack the `owlet` folder in there instead. `flashpoint doctor` prints the folder on its `owlet cache` line. The cache holds one file per hoot format, named `owlet-<version>-C<format>` (with `.exe` on Windows):

    | Season | Hoot format | File |
    |---|---|---|
    | 2024 | C6 | `owlet-1.0.1.1-C6` |
    | 2025 | C13 | `owlet-25.4.1-C13` |
    | 2026 | C19 | `owlet-26.3.0-C19` |

    These are the versions Flashpoint 0.2.0 uses. Delete the scratch lake afterwards.

6. Download the docs for your version: `flashpoint-docs-X.Y.zip` from the version's [GitHub Release](https://github.com/IgniteRobotics/flashpoint/releases), into `flashpoint-offline`.
7. Copy `flashpoint-offline` to the USB stick.

The folder now holds `wheels/`, `python.tar.gz` (or `.zip`), `uv.tar.gz` (or `.zip`), `owlet.tar.gz` (or `.zip`), and the docs zip.

## Install on the pit laptop (no internet)

### macOS and Linux

1. Copy the folder off the stick into your home folder (on Linux the stick is usually under `/media/`):

    ```sh
    cp -R /Volumes/<stick>/flashpoint-offline ~/
    ```

2. Put uv in `~/.local/bin`, and add that folder to your `PATH`:

    ```sh
    mkdir -p ~/.local/bin
    tar -xzf ~/flashpoint-offline/uv.tar.gz -C ~/.local/bin --strip-components=1
    ~/.local/bin/uv tool update-shell
    ```

    Open a new terminal.

3. Unpack Python where uv looks for it:

    ```sh
    mkdir -p "$(uv python dir)"
    tar -xzf ~/flashpoint-offline/python.tar.gz -C "$(uv python dir)"
    ```

4. Install Flashpoint from the folder, with no network:

    ```sh
    uv tool install --offline --no-index --find-links ~/flashpoint-offline/wheels --managed-python --python 3.13 flashpoint
    ```

5. Unpack owlet into Flashpoint's cache:

    ```sh
    mkdir -p ~/.cache/flashpoint
    tar -xzf ~/flashpoint-offline/owlet.tar.gz -C ~/.cache/flashpoint
    ```

6. Create your configuration:

    ```sh
    flashpoint init --team <number>
    ```

7. Unzip `flashpoint-docs-X.Y.zip` into a folder of its own, and open its `index.html` for these docs. Search may not work from disk; the navigation and links do.

### Windows

Run these in PowerShell. `E:` stands for the stick's drive letter.

1. Copy the folder off the stick into your home folder:

    ```powershell
    Copy-Item -Recurse E:\flashpoint-offline $HOME\
    ```

2. Put uv in `.local\bin` in your home folder, and add that folder to your `PATH`:

    ```powershell
    New-Item -ItemType Directory -Force $HOME\.local\bin
    Expand-Archive $HOME\flashpoint-offline\uv.zip -DestinationPath $HOME\.local\bin
    & "$HOME\.local\bin\uv.exe" tool update-shell
    ```

    Open a new PowerShell window.

3. Unpack Python where uv looks for it:

    ```powershell
    $py = uv python dir
    New-Item -ItemType Directory -Force $py
    Expand-Archive $HOME\flashpoint-offline\python.zip -DestinationPath $py
    ```

4. Install Flashpoint from the folder, with no network:

    ```powershell
    uv tool install --offline --no-index --find-links $HOME\flashpoint-offline\wheels --managed-python --python 3.13 flashpoint
    ```

5. Unpack owlet into Flashpoint's cache:

    ```powershell
    New-Item -ItemType Directory -Force $HOME\.cache\flashpoint
    Expand-Archive $HOME\flashpoint-offline\owlet.zip -DestinationPath $HOME\.cache\flashpoint
    ```

6. Create your configuration:

    ```powershell
    flashpoint init --team <number>
    ```

7. Unzip `flashpoint-docs-X.Y.zip` into a folder of its own, and open its `index.html` for these docs. Search may not work from disk; the navigation and links do.

### If the laptop already has Python 3.11 to 3.14: pip instead of uv

You can skip uv and the Python download, and install the wheels into a virtual environment with pip. Download the wheels in step 2 with that Python's version in `--python-version`, such as `3.12`. Then, on macOS and Linux:

```sh
python3.13 -m venv ~/flashpoint-venv
~/flashpoint-venv/bin/python -m pip install --no-index --find-links ~/flashpoint-offline/wheels flashpoint
```

On Windows:

```powershell
py -3.13 -m venv $HOME\flashpoint-venv
& $HOME\flashpoint-venv\Scripts\python.exe -m pip install --no-index --find-links $HOME\flashpoint-offline\wheels flashpoint
```

The command is then `~/flashpoint-venv/bin/flashpoint` (`$HOME\flashpoint-venv\Scripts\flashpoint.exe` on Windows), or plain `flashpoint` after you activate the environment. Unpack owlet as in step 5.

## Check it worked

```console
$ flashpoint --version
flashpoint 0.2.0
```

Then run `flashpoint doctor`. Its `python` line shows the Python 3.13 you brought, its `owlet cache` line shows the folder you unpacked into, and the owlet check says `downloaded, verified`. On a Mac it looks like this; Linux and Windows show `linuxx86-64` or `windowsx86-64` and their own home folder:

```text
python     3.13.<n> on macosuniversal
...
owlet cache /Users/<you>/.cache/flashpoint/owlet
...
  ok       owlet for hoot format C19    owlet 26.3.0 downloaded, verified
```

Before the first import, `doctor` checks only the newest hoot format Flashpoint knows. After you import hoots, it checks each format in the lake, so an older season's owlet shows up then.

Last, import one hoot with networking still off:

```sh
flashpoint ingest <a .hoot file>
```

It ends with `1 ingested` and no `FAILED` line. If it says `failed (owlet-download-failed), will retry`, the owlet for that hoot's format isn't in the cache: see [owlet isn't downloaded, or its checksum doesn't match](../troubleshooting.md#owlet-isnt-downloaded-or-its-checksum-doesnt-match). If `flashpoint` isn't found, see [`flashpoint` isn't found after installing](../troubleshooting.md#flashpoint-isnt-found-after-installing).
