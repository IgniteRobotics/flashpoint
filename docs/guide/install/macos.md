# Install on macOS

You need macOS 13 or later. Flashpoint brings its own Python, so you don't need to install one.

## Install

1. Install [uv](https://docs.astral.sh/uv/), once. With Homebrew:

    ```sh
    brew install uv
    ```

    Without Homebrew, use uv's installer: `curl -LsSf https://astral.sh/uv/install.sh | sh`. Open a new terminal afterwards.

2. Install Flashpoint:

    ```sh
    uv tool install --python 3.13 flashpoint
    ```

    uv downloads Python 3.13 for Flashpoint if your Mac doesn't have it. Nothing else on your Mac changes.

3. Create your configuration:

    ```sh
    flashpoint init --team <your team number>
    ```

    `init` asks for anything you leave out: your lake folder (where logs and results are stored, `~/flashpoint-lake` by default), your team name and the app's colours. It writes your config folder, `~/.config/flashpoint`. See [`flashpoint init`](../reference/cli.md#flashpoint-init).

## Check it worked

```sh
flashpoint --version
flashpoint doctor
```

`--version` prints `flashpoint` and a version number. `doctor` lists the setup checks: right after `init`, the robot check is a warning until you import this season's logs, which is expected.

If the terminal says `flashpoint: command not found`, run `uv tool update-shell` and open a new terminal.

## With pipx instead

If you already use [pipx](https://pipx.pypa.io/), it works too, but it needs a Python 3.11 to 3.14 already installed (Homebrew's newest Python may be too new):

```sh
pipx install --python python3.13 flashpoint
```

Use whichever of `python3.11` to `python3.14` you have.

## Before your first event

Flashpoint converts hoot files with CTRE's owlet tool, which it downloads the first time it sees a hoot of each format. Do that while you still have internet: ingest one hoot from this season's robot code, then check that `doctor` says owlet is `downloaded, verified`.

```sh
flashpoint ingest <a recent .hoot file>
flashpoint doctor
```

For a laptop that never goes online, see [Install without internet](../how-to/install-offline.md).

## Updating

```sh
uv tool upgrade flashpoint
```

With pipx, `pipx upgrade flashpoint`. Your config folder and lake are kept either way. To move from pipx to uv, run `pipx uninstall flashpoint`, then the uv install above; your config folder and lake aren't touched.

Next: [Quickstart](../quickstart.md).
