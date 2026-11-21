# Build info

Flashpoint reads a string entry named `MetaData` in the wpilog. Each value is one `Key: value` line. It uses `Project Name` to pick the robot file when a season has more than one robot (a practice bot and a competition bot), and also keeps `Git Branch` and `GitDirty`. With one robot per season you don't need it; with two and no build info, matches go to robot "unknown".

[`BuildInfo.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/BuildInfo.java) writes it from the [gversion](https://github.com/lessthanoptimal/gversion) Gradle plugin's `BuildConstants`, which many WPILib teams already use:

```java
  public static void log() {
    StringLogEntry meta = new StringLogEntry(DataLogManager.getLog(), "MetaData");
    meta.append("Project Name: " + BuildConstants.MAVEN_NAME);
    meta.append("Git Branch: " + BuildConstants.GIT_BRANCH);
    meta.append("GitDirty: " + (BuildConstants.DIRTY == 1 ? "Uncommitted changes" : "All changes committed"));
  }
```

Call `BuildInfo.log()` once in `robotInit()`, after `DataLogManager.start()` (the sample's `SignalLoggerSetup.start()` does that).

If you don't use gversion yet, the sample project's [`build.gradle`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/build.gradle) shows the plugin block. `MAVEN_NAME` is the Gradle project name, which is the project folder's name unless `settings.gradle` sets `rootProject.name`.

The project name must equal `project` in the robot file. **Draft robot** in the Setup view fills `project` in from the log.
