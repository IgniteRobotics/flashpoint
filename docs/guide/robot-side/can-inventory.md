# CAN inventory

The CAN inventory gives Flashpoint each CTRE device's factory serial number, so a motor's history follows the physical motor through swaps and rebuilds, not the CAN id it sits on.

The Phoenix diagnostics server runs inside your deployed robot program (port 1250) and lists every CTRE device with its serial number, the same list Tuner X shows. [`CanInventoryLogger.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/CanInventoryLogger.java) asks it once, a few seconds after boot, and writes one JSON string to `/Flashpoint/CANInventory`:

```json
{"schema": 1, "devices": [
  {"model": "Talon FX", "bus": "rio", "id": 5, "serial": "6E9415C3394C4853"},
  {"model": "Talon FX", "bus": "DriveTrain", "id": 11, "serial": "..."}
]}
```

- `model`, `bus`, `id` and `serial` are required for each device. Extra fields are kept but unused: the sample also logs `firmware`, which Flashpoint doesn't read yet ([#87](https://github.com/IgniteRobotics/flashpoint/issues/87)).
- If it can't read the list, write `{"schema": 1, "error": "<why>"}` instead; Flashpoint records the error and uses swap dates for that log.
- Log it again after a hot swap (for example from a dashboard button), and Flashpoint splits the motor's history at that moment.

```java
/*
 * A suggestion, not a library: copy it into your robot project and adapt it.
 * Tested with WPILib 2026.2.1, Phoenix 6 26.3.0
 * Docs: https://igniterobotics.github.io/flashpoint/latest/robot-side/can-inventory/
 */
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

Before you rely on it, open `http://<roboRIO address>:1250/?action=getdevices` in a browser while the robot program runs. Check that the field names match (`DeviceArray`, `Model`, `CANbus`, `ID`, `SerialNo`). The diagnostics server isn't a documented API, and the names can change between Phoenix versions. When the inventory is read, History follows each motor by its serial number (its unit is `ctre:<serial>`) instead of by slot. Without it, motors get legacy units ([how units work](../under-the-hood/how-it-works.md)).
