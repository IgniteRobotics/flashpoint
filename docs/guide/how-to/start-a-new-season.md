# Start a new season

Flashpoint doesn't need to be reinstalled each year, but a new season needs a few things before its first event: a Flashpoint that reads the new hoots, a season file, and a robot file for the new robot. Last season's logs, robot file and motor history stay as they are.

## Before you start

- Kickoff has happened, and the new robot code runs and writes logs (a `.wpilog` and its hoots).
- Internet, for the update and for the first hoot of the season (owlet).
- For 2027: Flashpoint 0.3.1 or later. See [The 2027 season](#the-2027-season) below.

## Steps

Do these after kickoff, when the new robot code first runs.

1. **Update Flashpoint.** A new season's CTRE hoot format needs a Flashpoint that knows its owlet.

    ```sh
    uv tool upgrade flashpoint
    ```

    If you installed with pipx, run `pipx upgrade flashpoint` instead. Your config folder and lake are kept ([Update Flashpoint](update-flashpoint.md)). An older Flashpoint quarantines the new hoots with `unsupported-compliancy:<n>`, and `doctor` says:

    ```text
      PROBLEM  owlet for hoot format C<n>   the lake has C<n> hoots, which this Flashpoint can't read
    ```

    If that happened, update, then retry those files from the lake's own copies:

    ```sh
    flashpoint rebuild --quarantined
    ```

2. **Check the season file.** Flashpoint ships a season file for each year it knows: this version ships 2025, 2026 and 2027. `flashpoint init` copies only the current season and earlier ones into your config folder, so a later year (2027, for a configuration made in 2026) is read from Flashpoint itself, and a fix to it in a Flashpoint update reaches you.
    - To change a root (for example to add your robot code's `robot` root), copy that year's file into `seasons/` in your config folder. Your copy then wins, and later fixes to the bundled file no longer reach it.
    - For a year Flashpoint doesn't ship yet, copy `seasons/<last year>.toml` to `seasons/<this year>.toml` and change `season =`.
    - Without a season file, a robot file that declares signals doesn't load; `doctor` names the file to create.

    The [season file reference](../reference/config/seasons.md) has every key.

3. **Add the robot file.** Ingest a log from the new robot, with internet so owlet downloads:

    ```sh
    flashpoint ingest <folder>
    ```

    The new season has no robot file yet, so the import drafts one, `robots/<year>-comp.toml` (then `<year>-practice` for a second robot-code project), with this season's year and project name. Name its slots in **Setup > Robots**, or use **Draft robot** in the Setup view for hoot-only logs ([Set up a robot file](set-up-a-robot-file.md)). Last season's robot file stays, so its logs keep their mapping.

    A log's season is the calendar year it was recorded in. Logs from December testing of next season's robot count as this season, so give that robot a file for this season too, or wait until January ([Which robot file a log uses](../reference/config/robots.md#which-robot-file-a-log-uses)).

4. **Check the robot code still writes** the [CAN inventory, bus status and build info](../robot-side/index.md) entries. A new robot project is easy to start without them.

5. **Keep the motor list.** The motor list (`units/units-seed.csv`) carries over. Add the new season's motors, and don't remove or relabel old ones: a motor reused on the new robot keeps its history by serial number ([Units](../reference/config/units.md)).

6. **Run `flashpoint doctor`.** From January 1 it checks for a robot for the new year. Until the lake has logs from the new year, that check is a warning that says so, not a problem:

    ```text
      WARN     Robot for 2027               no robot configured for season 2027; the lake has no 2027 logs yet (the first import of this season's logs drafts one)
    ```

    Once the lake has logs from the new year, it's a problem until a robot is configured.

## Check it worked

After your first import of the new season, `flashpoint doctor` shows:

- the robot check ok, naming the new robot, or a warning about unnamed slots if you haven't named them yet:

    ```text
      ok       Robot for 2027               2027-comp
    ```

- an owlet check for the format your new hoots use, downloaded:

    ```text
      ok       owlet for hoot format C<n>   owlet <version> downloaded, verified
    ```

- the new robot under `robots`, with `season 2027` and the new project name.

The new season's matches are in Replay, and History still has last season's motors.

If the owlet check is a problem, see [Hoots this version can't read](../troubleshooting.md#hoots-this-version-cant-read). If the robot check is a problem, see [No robot for this season](../troubleshooting.md#no-robot-for-this-season).

## The 2027 season

This version of Flashpoint is ready for January 1, 2027, but not for 2027 hoots:

- **2027 hoots need Flashpoint 0.3.1.** It adds the owlet for the 2027 hoot format, and is planned for kickoff week (2027-01-16). Until you update, the new hoots are quarantined and `doctor` says the lake "has C*n* hoots, which this Flashpoint can't read" (step 1).
- **The 2027 season file ships**, with the 2026 roots and `season = 2027`. Any WPILib 2027 root changes come in 0.3.1. A configuration made in 2026 reads 2027 from Flashpoint itself, so that fix reaches you when you update, unless you copied `seasons/2027.toml` into your config folder.
- **`doctor` stays green on January 1.** "Robot for 2027" is a warning while the lake has no 2027 logs. The first 2027 import drafts the robot.
- **Live CAN** uses a NetworkTables library held below its 2027 release until 0.3.1 ([Live CAN health](live-can-health.md)).
- **Systemcore**, the 2027 control system, is untested ([#110](https://github.com/IgniteRobotics/flashpoint/issues/110)).

The [support matrix](../support-matrix.md#the-2027-season-and-systemcore) has the current status, and [#73](https://github.com/IgniteRobotics/flashpoint/issues/73) tracks the season.
