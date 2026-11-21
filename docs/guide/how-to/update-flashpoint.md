# Update Flashpoint

Update to a new release, and roll back if you need to. Updating replaces the program only: your config folder and your lake are kept, and you don't import anything again.

## Before you start

- Internet on the laptop. Without it, see [Install without internet](install-offline.md).
- Read the release notes for the new version: the [GitHub Releases](https://github.com/IgniteRobotics/flashpoint/releases) page or the [changelog](https://github.com/IgniteRobotics/flashpoint/blob/main/CHANGELOG.md). They say when a release needs `flashpoint rebuild`, or extra steps to go back to an older version.
- Note the version you have now, in case you want to go back:

    ```sh
    flashpoint --version
    ```

- Stop `flashpoint serve` (Ctrl-C) and a running `flashpoint acquire --watch`. A running command keeps using the old version until it's started again.

## Update

1. Update with the tool you installed with. With uv, on every computer:

    ```sh
    uv tool upgrade flashpoint
    ```

    With pipx on macOS or Linux:

    ```sh
    pipx upgrade flashpoint
    ```

    With pipx on Windows:

    ```powershell
    py -3.13 -m pipx upgrade flashpoint
    ```

2. Check the setup:

    ```sh
    flashpoint doctor
    ```

    If the lake section says ``<n> file(s) from an older pipeline version: run `flashpoint rebuild` ``, or the release notes say to, run `flashpoint rebuild`. It reprocesses those files from the lake's own copies, so you don't need the original logs, then rebuilds Replay data.

    ```sh
    flashpoint rebuild
    ```

3. Start `flashpoint serve` again, and the watch if you use it. As a service, restart it: `systemctl --user restart flashpoint-acquire` on Linux, or `schtasks /End /TN flashpoint-acquire` then `schtasks /Run /TN flashpoint-acquire` on Windows ([Automatic import](auto-import.md)).

A new season's Phoenix release can change the hoot format. After updating for a new season, ingest one hoot from this season's robot code while you have internet, so Flashpoint downloads the owlet it needs ([User guide: Before an event](../user-guide.md#before-an-event-get-ready-offline)).

If you installed from a checkout, update with `git pull`, then `uv tool install --force --python 3.13 .` in the checkout.

## What updating changes

| | After an update |
|---|---|
| Config folder | Not touched. A settings file you never copied there keeps using the default that ships with the new version |
| Lake | Not touched by the update itself. A new version may update the lake's records the first time it opens the lake: 0.2.0, for example, marked files quarantined for a machine error as `failed`, so they're retried. The release notes say when |
| owlet | Kept in `~/.cache/flashpoint/` (or `FLASHPOINT_CACHE`). A version that reads a new hoot format downloads its owlet on the first such hoot |

## Roll back

To go back to an older version, install it by number. With uv:

```sh
uv tool install --python 3.13 flashpoint==<version>
```

uv then keeps that version: `uv tool upgrade` says it's pinned. To return to the newest release:

```sh
uv tool install --python 3.13 flashpoint@latest
```

With pipx:

```sh
pipx install --force flashpoint==<version>
```

Check the release notes of the version you're leaving before you go back. Going from 0.2.0 to an older version, for example, needs a change to the lake first, given in the 0.2.0 notes under "Downgrading".

## Check it worked

`flashpoint --version` prints the new version:

```console
$ flashpoint --version
flashpoint <version>
```

The first line of `flashpoint doctor` shows the same version and the pipeline version, such as `flashpoint 0.2.0 (pipeline v2)`, and the lake section has no line about files from an older pipeline version.

If `flashpoint` isn't found after the update, see [`flashpoint` isn't found after installing](../troubleshooting.md#flashpoint-isnt-found-after-installing). If `doctor` says `the lake has C<number> hoots, which this Flashpoint can't read`, see [Hoots this version can't read](../troubleshooting.md#hoots-this-version-cant-read).
