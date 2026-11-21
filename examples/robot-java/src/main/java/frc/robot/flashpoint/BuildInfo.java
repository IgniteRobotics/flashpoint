/*
 * A suggestion, not a library: copy it into your robot project and adapt it.
 * Tested with WPILib 2026.2.1, Phoenix 6 26.3.0
 * Docs: https://igniterobotics.github.io/flashpoint/latest/robot-side/build-info/
 */
package frc.robot.flashpoint;

import edu.wpi.first.util.datalog.StringLogEntry;
import edu.wpi.first.wpilibj.DataLogManager;
import frc.robot.BuildConstants;

/** Logs the robot code's build info to the wpilog's MetaData entry, one "Key: value" per line. */
public final class BuildInfo {
  private BuildInfo() {}

  /** Call once from robotInit(), after DataLogManager.start(). */
  public static void log() {
    StringLogEntry meta = new StringLogEntry(DataLogManager.getLog(), "MetaData");
    meta.append("Project Name: " + BuildConstants.MAVEN_NAME);
    meta.append("Git Branch: " + BuildConstants.GIT_BRANCH);
    meta.append("GitDirty: " + (BuildConstants.DIRTY == 1 ? "Uncommitted changes" : "All changes committed"));
  }
}
