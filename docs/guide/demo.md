# Demo

Look around Flashpoint's Replay view before you install anything:

**[Open the hosted demo](demo/replay/index.html)**

It's real data: five matches from team 6829's (Ignite Robotics) 2025 and 2026 robots, from the Georgia district events. The header reads "Sample data · 6829" so you can tell it apart from your own.

## What it is

- **Replay only.** It's a static export (`flashpoint report --static`), the same thing [you'd put on a USB stick](how-to/replay-on-a-usb-stick.md). History, Anomalies, Setup and Live CAN need `flashpoint serve`, so they aren't in it.
- **No raw logs.** The export leaves out the `.wpilog` and `.hoot` files, so there's nothing to download or open in AdvantageScope.
- **Nothing to set up.** It runs in your browser and talks to no server but this site.

Start with **Q7** (2026, Georgia district championship): a full qualification match with tracks, markers and every motor in the readout. The seasons filter starts on the latest season; pick **All seasons** to see the 2025 matches.

The other four show what real logs can look like, and what Replay says about it:
- **E10**: the robot code restarted during the match, so its Replay covers only the part before the restart.
- **P2** and **Q30**: wpilogs with no motor data to line up, marked **no aligned samples**.
- **Q19**: hoots with no wpilog, also **no aligned samples**.

[A match says "no aligned samples"](troubleshooting.md#a-match-says-no-aligned-samples) explains each case.

## Try it on your own logs

The [Quickstart](quickstart.md) installs Flashpoint and gets your own match into Replay with three commands, or tries it on a sample match first.
