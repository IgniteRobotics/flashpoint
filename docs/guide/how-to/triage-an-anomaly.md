# Triage an anomaly

**Status: beta** ([#77](https://github.com/IgniteRobotics/flashpoint/issues/77)). Detection is tuned on one team's robots (ours), so a finding is a suggestion to check against its evidence, not a verdict.

Triage records what the team did about something detection flagged: someone is looking into it, it was fixed, or it's expected. The note you write is shown the next time the same thing turns up on that motor, so the next student starts from what you found.

## Before you start

- The lake has matches, and detection has run on them. `ingest` and the automatic import run it for you; to run it by hand, use `flashpoint detect`. Until it has run, the Anomalies view says "Detection has not run on this lake yet: run `flashpoint detect`."
- You're on the laptop running `flashpoint serve`, started the usual way, and the app is open on that laptop. Triage is saved in the lake, so it only works there. Shared with `--host 0.0.0.0`, the buttons are replaced by "Read-only: triage is only available on the machine running Flashpoint.", for everyone, the laptop included.
- You can get to the robot, or ask someone who can: triage is about what you found on it.

## Triage it

1. Open the **ANOMALIES** tab. **SHOW** is on **OPEN**: the anomalies that are NEW or INVESTIGATING, highest severity first.
2. Pick one in the queue. Read its title, **ACTUAL** and **EXPECTED**, and **WHY IT WAS FLAGGED**.
3. Check the evidence. The **EVIDENCE** chart shows the metric over the unit's matches, with flagged matches as large dots. Select a point to open that match in Replay, with this slot's tracks, and see what happened.
4. Read **LIKELY CAUSES** and **SEEN THIS BEFORE**: what to check for this kind of finding, and what the team wrote last time.
5. Press **INVESTIGATE** when someone starts looking. It saves at once.
6. When you know the answer, close it:
    - **MARK RESOLVED** if you found and fixed something. Write what in **WHAT WAS FOUND OR FIXED**, such as "Loose power lug on the intake Talon, re-crimped".
    - **DISMISS AS EXPECTED** if it isn't a problem. Write why in **WHY THIS IS EXPECTED**, such as "New gearbox installed before Q12".
7. Press **SAVE ·** and the action. The text is required, up to 500 characters.

To bring a closed anomaly back, open it under **RESOLVED** or **DISMISSED** and press **REOPEN**. It goes back to NEW.

### After a deliberate change: restart the baseline

DRIFT and STEP compare a unit with its own earlier matches. After you change the mechanism on purpose (a new gearbox, a new ratio), the old normal keeps flagging the new one. When you dismiss such an anomaly, tick "Restart this unit's baseline for _metric_ from the onset match (for example after a gearbox change)". The unit's baseline for that metric then starts again from the match the anomaly started in.

A restart runs `flashpoint detect` in the background. Until it has run, the anomaly says "Baseline restart pending: detection has not run since the restart was recorded. The queue updates when it does." The unit needs `min_baseline` matches again (5 by default, [anomaly.toml](../reference/config/anomaly.md#baseline-drift-and-step)) before DRIFT and STEP judge it.

### Add a cause you found

**LIKELY CAUSES** comes from `anomaly-causes.toml`. When you resolve an anomaly and the cause wasn't listed, add it so the next student sees it:

1. Open `anomaly-causes.toml` in your config folder. `flashpoint init` puts a copy there; if yours has none, copy the [starting list](https://github.com/IgniteRobotics/flashpoint/blob/main/src/flashpoint/defaults/anomaly-causes.toml) in first.
2. Add a `[[cause]]` with the anomaly's method and metric, as the detail shows them, and the subsystem if it applies only there:

    ```toml
    [[cause]]
    method = "STEP"
    metric = "stator_current_p95"
    subsystem = "intake"
    text = "Something rubbing or jammed in the intake"
    ```

3. Run `flashpoint doctor` to check the file loads. The next anomaly you open shows the new cause; no restart is needed.

Every key is in the [`anomaly-causes.toml` reference](../reference/config/anomaly-causes.md).

## Check it worked

- After **INVESTIGATE**, the anomaly's state in the queue and the header reads **INVESTIGATING**.
- After **SAVE**, it moves out of **OPEN**: **SHOW** counts it under **RESOLVED** or **DISMISSED**. Its detail has an **OUTCOME** with what you wrote, and **HISTORY** lists the change with its time ("baseline restarted" when you ticked the box).
- A refused save shows "Not saved:" and the reason, such as "an outcome is required: say what was found or done".

If `doctor` shows `anomaly-causes.toml` doesn't load, or the detail says "The cause catalogue has an error:", see [A settings file doesn't load](../troubleshooting.md#a-settings-file-doesnt-load). Every part of the view is in the [Anomalies view reference](../reference/views/anomalies.md).
