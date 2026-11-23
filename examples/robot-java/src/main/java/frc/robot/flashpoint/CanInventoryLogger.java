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
