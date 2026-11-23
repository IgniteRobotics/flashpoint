# Set up a robot file

A robot file tells Flashpoint which motor is which. You rarely write one from scratch: Flashpoint drafts it from a log, and you name the motors. This page is the order to do it in. Every key is in the [robot file reference](../reference/config/robots.md).

## Before you start

- Flashpoint installed, and a configuration made with `flashpoint init` ([Install](../install/index.md)). Without a configuration, nothing is drafted.
- One match's logs from the robot: the `.wpilog` and its `.hoot` files ([Import logs from a USB stick](import-from-usb.md)). The draft on import needs both. **Draft robot** in the Setup view needs only the hoots.
- Internet the first time you import a hoot, so Flashpoint can download owlet, CTRE's hoot converter.
- Someone who knows the robot: which CAN id is which motor.

## Steps

1. Import a match from the robot:

    ```sh
    flashpoint ingest <folder>
    ```

    If the log's season has no robot file yet, the import drafts one from the match with the most CAN devices, and says so:

    ```text
    drafted robot 2026-comp from 2026gacmp_qm7 (22 devices) into /home/pit/.config/flashpoint/robots/2026-comp.toml: name each slot's subsystem and role in Setup > Robots
    ```

    It drafts one file per robot-code project: `<season>-comp`, then `<season>-practice` for a second project. If you see this line, go to step 3.

2. If nothing was drafted, use **Draft robot**. That happens with hoot-only logs, with `flashpoint ingest --no-robot-draft`, and for a second robot in a season that already has a robot file: a season with any robot file is never drafted into.
    1. Run `flashpoint serve` and open <http://127.0.0.1:8000/#view=setup&tab=draft>.
    2. Pick an imported log in the list, then press **DRAFT FROM THIS LOG**. Every CAN device in its hoots becomes a slot.
    3. Check **ROBOT ID**. It's also the file's name, such as `2026-practice`.
    4. Press **SAVE ROBOT**. The lake rebuilds in the background: "Saved _file_. Rebuilding the lake in the background; reload Replay when it finishes (a minute or two)."

    The [Setup view reference](../reference/views/setup.md#draft-robot) explains each line of the draft.

3. Name the slots. In Setup, open **CONFIGURATION** and find your robot under **ROBOTS**. Each drafted slot has an id made of its model and CAN id, such as `talonfx-11`, and an empty **SUBSYSTEM** and **ROLE**. Fill them in: subsystem `drivetrain`, role `front-left drive`. Keep the drafted slot ids. Press **SAVE**.

    You can name them later. Until you do, History and anomalies name those slots by CAN id. To edit the file in a text editor instead, it's in the `robots/` folder of your config folder (`~/.config/flashpoint/robots/` on macOS and Linux, `%APPDATA%\flashpoint\robots\` on Windows).

4. Tie the robot code's bus names to the hoot's buses, if the robot code logs [CAN bus status](../robot-side/bus-status.md). The draft fills these in for the rio and a single CANivore. With more than one CANivore, add a row for each in the **BUSES** table, or a `[[bus]]` table in the file ([`[[bus]]`](../reference/config/robots.md#bus-the-robot-codes-bus-names)).

5. Add signals, if you want a NetworkTables or wpilog value in Flashpoint, such as a subsystem's state. Use the **SIGNALS** table in Setup, or `[[signal]]` tables in the file ([`[[signal]]`](../reference/config/robots.md#signal-a-value-from-the-log)). A signal's `root` must be defined in the [season file](../reference/config/seasons.md); the bundled one leaves the `robot` root commented out until you set it.

6. Group motors that do the same job, so the SIBLING anomaly method can compare them. Setup has no table for this, so edit the file:

    ```toml
    [[siblings]]
    name = "drive"
    slots = ["talonfx-11", "talonfx-12", "talonfx-13", "talonfx-14"]
    ```

    See [`[[siblings]]`](../reference/config/robots.md#siblings-slots-compared-with-each-other).

7. Pick a battery proxy: a low-current device on the main power bus, such as the gyro. Replay's battery track and the brownout markers use its supply voltage. Add the key near the top of the file, above the first `[[slot]]`, so TOML reads it as a top-level key:

    ```toml
    battery_proxy = "pigeon2-1"
    ```

    Don't pick a motor ([The battery proxy](../reference/config/robots.md#the-battery-proxy)).

8. After any edit to the file, or a **SAVE** in Setup, rebuild every session with the new mapping:

    ```sh
    flashpoint derive --all
    ```

    You don't need to import the logs again.

## Check it worked

Run `flashpoint doctor`. The `robots` section lists the file, its season, its project and its counts:

```text
robots     /home/pit/.config/flashpoint/robots
  2026-comp      season 2026 project Robot-2026: 22 slots, 0 signals
```

The robot check names it:

```text
  ok       Robot for 2026               2026-comp
```

While a drafted file still has slots with no subsystem, it's a warning instead, and says how many:

```text
  WARN     Robot for 2026               2026-comp: 4 of 22 slots unnamed (drafted automatically): History and anomalies name them by CAN id
                                        fix: name each slot's subsystem and role in Setup > Robots
```

In Replay, the match no longer shows robot "unknown".

If the file doesn't load, `doctor` names the file and the key: see [A settings file doesn't load](../troubleshooting.md#a-settings-file-doesnt-load) and the robot file's [common errors](../reference/config/robots.md#common-errors). If matches still show robot "unknown", see [The robot shows as "unknown"](../troubleshooting.md#the-robot-shows-as-unknown).
