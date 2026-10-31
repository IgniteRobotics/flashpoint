# Robot-side logging

Flashpoint reads the logs your robot already writes. Two small additions to robot code make it much more useful:

| Logger | What Flashpoint gets | Without it |
|---|---|---|
| **CAN inventory** | Each motor's factory serial number, so its history follows the physical motor through swaps and rebuilds | History is kept per slot (CAN id), split at the swap dates you declare in the robot file |
| **CAN bus status** | Bus utilisation and error counts per bus, per match phase, in CAN health | Those columns read "not logged" |
| **Build info** | The robot code's project name, which picks the robot file when a season has more than one robot (a practice bot and a comp bot) | Works with one robot per season. With two, matches go to robot "unknown" |

The first two write to the WPILib DataLog (`.wpilog`) under `/Flashpoint/`, the season root named `flashpoint`; build info uses a `MetaData` entry ([below](#build-info)). The samples below are suggestions, not a library: copy them into your robot project and adapt them. Check them against your Phoenix 6 version before an event.

These samples assume `DataLogManager.start()` runs in `robotInit()`, as most WPILib robots already do.

To watch CAN dropouts live during shake and drive tests, robot code also publishes running totals over NetworkTables: see [Live CAN health](live-can-health.md).

## CAN inventory

The Phoenix diagnostics server runs inside your deployed robot program (port 1250) and lists every CTRE device with its serial number, the same list Tuner X shows. The logger asks it once, a few seconds after boot, and writes one JSON string to `/Flashpoint/CANInventory`:

```json
{"schema": 1, "devices": [
  {"model": "Talon FX", "bus": "rio", "id": 5, "serial": "6E9415C3394C4853"},
  {"model": "Talon FX", "bus": "DriveTrain", "id": 11, "serial": "..."}
]}
```

- `model`, `bus`, `id` and `serial` are required for each device. Extra fields are kept but unused.
- If it can't read the list, write `{"schema": 1, "error": "<why>"}` instead; Flashpoint records the error and uses swap dates for that log.
- Log it again after a hot swap (for example from a dashboard button), and Flashpoint splits the motor's history at that moment.

```java
package frc.robot.flashpoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.wpi.first.util.datalog.StringLogEntry;
import edu.wpi.first.wpilibj.DataLogManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Logs every CTRE device's serial number to /Flashpoint/CANInventory (Flashpoint schema 1). */
public final class CanInventoryLogger {
  private static final URI DEVICES = URI.create("http://localhost:1250/?action=getdevices");
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final StringLogEntry ENTRY =
      new StringLogEntry(DataLogManager.getLog(), "/Flashpoint/CANInventory");

  private CanInventoryLogger() {}

  /** Call once from robotInit(). Runs in the background; never blocks the robot loop. */
  public static void start() {
    Thread thread = new Thread(CanInventoryLogger::run, "CanInventoryLogger");
    thread.setDaemon(true);
    thread.start();
  }

  private static void run() {
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    String lastError = "no attempt";
    // Devices take a few seconds to enumerate after boot: retry for about a minute.
    for (int attempt = 0; attempt < 12; attempt++) {
      try {
        Thread.sleep(5000);
        HttpRequest request = HttpRequest.newBuilder(DEVICES).timeout(Duration.ofSeconds(5)).build();
        String body = client.send(request, HttpResponse.BodyHandlers.ofString()).body();
        ArrayNode devices = toInventory(JSON.readTree(body));
        if (devices.size() > 0) {
          ObjectNode inventory = JSON.createObjectNode().put("schema", 1);
          inventory.set("devices", devices);
          ENTRY.append(JSON.writeValueAsString(inventory));
          return;
        }
        lastError = "the diagnostics server listed no devices";
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        lastError = e.toString();
      }
    }
    ENTRY.append(JSON.createObjectNode().put("schema", 1).put("error", lastError).toString());
  }

  /** The diagnostics server's DeviceArray, as Flashpoint's device list. */
  private static ArrayNode toInventory(JsonNode response) {
    ArrayNode out = JSON.createArrayNode();
    for (JsonNode device : response.path("DeviceArray")) {
      String serial = device.path("SerialNo").asText("");
      if (serial.isEmpty()) {
        continue; // not a device that reports a serial
      }
      out.addObject()
          .put("model", device.path("Model").asText())
          .put("bus", device.path("CANbus").asText("rio"))
          .put("id", device.path("ID").asInt())
          .put("serial", serial)
          .put("firmware", device.path("CurrentVers").asText(""));
    }
    return out;
  }
}
```

Call `CanInventoryLogger.start();` at the end of `robotInit()`.

Before you rely on it, open `http://<roboRIO address>:1250/?action=getdevices` in a browser while the robot program runs. Check that the field names match (`DeviceArray`, `Model`, `CANbus`, `ID`, `SerialNo`). The diagnostics server isn't a documented API, and the names can change between Phoenix versions. After a match, `flashpoint doctor` and the History view show motors as `ctre:<serial>` when the inventory was read.

## CAN bus status

Phoenix 6 reports each bus's health. The logger writes five values per bus, every half second, under `/Flashpoint/CANBus/<name>/`, where `<name>` is the bus name your robot code uses (`rio`, or the CANivore's name such as `DriveTrain`):

| Entry | Type | Meaning |
|---|---|---|
| `Utilization` | double | Fraction of the bus in use, 0 to 1 |
| `BusOffCount` | integer | Times the bus went off (a running count) |
| `TxFullCount` | integer | Times the transmit buffer was full (a running count) |
| `ReceiveErrorCount` | integer | Receive error counter |
| `TransmitErrorCount` | integer | Transmit error counter |

```java
package frc.robot.flashpoint;

import com.ctre.phoenix6.CANBus;
import com.ctre.phoenix6.CANBus.CANBusStatus;
import edu.wpi.first.util.datalog.DataLog;
import edu.wpi.first.util.datalog.DoubleLogEntry;
import edu.wpi.first.util.datalog.IntegerLogEntry;
import edu.wpi.first.wpilibj.DataLogManager;
import edu.wpi.first.wpilibj.Notifier;
import java.util.List;

/** Logs CAN bus status to /Flashpoint/CANBus/<name>/ every half second. */
public final class CanBusStatusLogger {
  private record Bus(
      CANBus bus,
      DoubleLogEntry utilization,
      IntegerLogEntry busOff,
      IntegerLogEntry txFull,
      IntegerLogEntry rec,
      IntegerLogEntry tec) {}

  private final List<Bus> buses;
  private final Notifier notifier = new Notifier(this::log);

  /** names: the buses as your robot code names them, e.g. "rio" and "DriveTrain". */
  public CanBusStatusLogger(String... names) {
    DataLog log = DataLogManager.getLog();
    buses = java.util.Arrays.stream(names)
        .map(name -> {
          String prefix = "/Flashpoint/CANBus/" + name + "/";
          return new Bus(
              new CANBus(name),
              new DoubleLogEntry(log, prefix + "Utilization"),
              new IntegerLogEntry(log, prefix + "BusOffCount"),
              new IntegerLogEntry(log, prefix + "TxFullCount"),
              new IntegerLogEntry(log, prefix + "ReceiveErrorCount"),
              new IntegerLogEntry(log, prefix + "TransmitErrorCount"));
        })
        .toList();
    notifier.setName("CanBusStatusLogger");
    notifier.startPeriodic(0.5);
  }

  private void log() {
    for (Bus b : buses) {
      CANBusStatus status = b.bus().getStatus();
      if (!status.Status.isOK()) {
        continue; // skip a failed read rather than logging zeros
      }
      b.utilization().append(status.BusUtilization);
      b.busOff().append(status.BusOffCount);
      b.txFull().append(status.TxFullCount);
      b.rec().append(status.REC);
      b.tec().append(status.TEC);
    }
  }
}
```

Create it once in `robotInit()`, for example `new CanBusStatusLogger("rio", "DriveTrain");`, and keep a reference so the `Notifier` isn't garbage collected.

Then tell Flashpoint about the bus names in your robot file. In the Setup view, the robot's **BUSES** table ties each name to its hoot bus (`rio`, or `canivore`). Declare the five signals per bus under the `flashpoint` root, as `robots/2026-comp.toml` does for Ignite. A bus whose status isn't logged reads "not logged" in CAN health, never zero.

## Build info

Flashpoint reads a string entry named `MetaData` in the wpilog. Each value is one `Key: value` line. It uses `Project Name` to pick the robot file, and also keeps `Git Branch` and `GitDirty`. Ignite writes these from the [gversion](https://github.com/lessthanoptimal/gversion) Gradle plugin's `BuildConstants`, which many WPILib teams already use:

```java
StringLogEntry meta = new StringLogEntry(DataLogManager.getLog(), "MetaData");
meta.append("Project Name: " + BuildConstants.MAVEN_NAME);
meta.append("Git Branch: " + BuildConstants.GIT_BRANCH);
meta.append("GitDirty: " + (BuildConstants.DIRTY == 1 ? "Uncommitted changes" : "All changes committed"));
```

Log it once in `robotInit()`, after `DataLogManager.start()`. The project name must equal `project` in the robot file. **Draft robot** in the Setup view fills `project` in from the log.
