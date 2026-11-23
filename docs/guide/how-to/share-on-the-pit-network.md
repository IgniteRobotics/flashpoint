# Show the app on the pit network

`flashpoint serve --host 0.0.0.0` lets other devices on the same network, such as a mentor's laptop or a drive coach's tablet, open the app from the pit laptop. They see the same lake, read-only.

**There's no password**: anyone on that network who knows the laptop's address can see everything in the lake. The app is read-only then (for everyone, including the laptop itself), so nobody can change triage or settings, but they can read and download your logs. Only do it on a network you trust, and stop it (Ctrl-C) when you're done. [Privacy and network](../privacy.md) lists what a lake holds.

## Before you start

- `flashpoint serve` works on the laptop on its own ([User guide: Look at the data](../user-guide.md#look-at-the-data-flashpoint-serve)).
- The laptop and the other devices are on the same network, and it's one you trust.
- Do any triage, Setup changes or AdvantageScope launches first, or after you stop sharing. None of them work while the app is shared.

## Share it

1. Stop any `flashpoint serve` already running (Ctrl-C in its terminal).
2. Start it shared:

    ```sh
    flashpoint serve --host 0.0.0.0
    ```

    If the laptop's firewall asks whether to allow incoming connections, allow them, or the other devices can't reach it.

3. Find the laptop's IP address on that network.

    macOS (`en0` is usually Wi-Fi; if it prints nothing, try `en1`):

    ```sh
    ipconfig getifaddr en0
    ```

    Linux (the first address is usually the one):

    ```sh
    hostname -I
    ```

    Windows: run `ipconfig` and use the **IPv4 Address** under the adapter that's on the pit network.

    ```powershell
    ipconfig
    ```

4. On the other device, open `http://<laptop IP>:8000/` in a browser, for example `http://192.168.1.20:8000/`.
5. When you're done, press Ctrl-C in the terminal. To make changes again, start `flashpoint serve` without `--host`.

To use another port, add `--port`, such as `flashpoint serve --host 0.0.0.0 --port 8080`, and put that port in the address instead of `8000`.

## What others can and can't do

| Works for everyone | Doesn't work, for anyone, the laptop included |
|---|---|
| Replay, History, Anomalies and Setup, to look at | Triage: "Read-only: triage is only available on the machine running Flashpoint." |
| Downloading raw logs from Replay | Setup fixes and saves, and Appearance changes |
| Live CAN, which changes nothing. The laptop holds the one connection to the robot | **OPEN IN ADVANTAGESCOPE**: "Open in AdvantageScope is only available on the machine running Flashpoint. Use the downloads below." |

Sensitivity and grouping in Anomalies, and marks in Live CAN, are each viewer's own. In Replay, **COPY LINK** can't reach the clipboard on a plain `http://` address from another device: it still reads **COPIED**, so copy the address bar instead.

## Check it worked

When it starts, `serve` prints a warning and then the address it serves on:

```text
WARNING: serving on 0.0.0.0 shares the lake's data and raw logs with everyone on the pit network, read-only and with no password.
Flashpoint (lake <lake>) at http://127.0.0.1:8000/
```

The banner shows `127.0.0.1`, the laptop's own address. Other devices use the laptop's IP from step 3.

On the other device, the app opens on Replay with your matches. Open **SETUP**: under the band it says "Read-only: this app is shared on the network (or not opened on the computer running it). Run the commands shown on that computer."

If `serve` says `cannot listen on 0.0.0.0:8000:` and a reason, another program has the port: use `--port`. For anything else, start at [Troubleshooting](../troubleshooting.md).
