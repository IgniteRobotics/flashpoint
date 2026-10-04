# CTRE FRC Devices: Unique Serial Numbers and How to Log Them

**Short answer:** Yes, every CTRE CAN device has a unique factory ID that survives firmware flashing and CAN ID or name changes. It is not logged automatically, but you can log it yourself.

## The ID

Every CTRE CAN device carries a factory serial number. It is independent of the CAN ID and the device name. Phoenix Tuner X shows it on the Device Details page, next to the Name, ID, Firmware Version, and Model (CTR Electronics, n.d.-e).

The serial is set at manufacture. Firmware upgrades, CAN ID or name changes, and factory defaults do not affect it. It is also how Tuner tells apart two devices that share a CAN ID.

The diagnostics server also reports other factory data alongside the serial: manufacture date, hardware revision, and bootloader revision.

## Not in the Robot API or Hoot Logs

As far as I know, the Phoenix 6 device classes do not expose the serial to robot code. What you do get:

- The CAN ID and the bus the device is on
- The firmware Version status signals
- A hash code

The hash code is derived from the device type and CAN ID, not from the physical hardware. CTRE documents it as "not unique across networks" (CTR Electronics, n.d.-g).

Hoot logs key signals by model, ID, and bus. If you swap a motor and give the replacement the same CAN ID, the log cannot tell the two units apart.

## How to Log It Yourself

Tuner gets the serial from the **Phoenix Diagnostics Server**, which runs inside your deployed robot program on port 1250 (CTR Electronics, n.d.-f). CTRE's documentation notes that this HTTP API can be used by third-party software or by the robot application itself (CTR Electronics, n.d.-h).

The relevant endpoint is:

```
http://<rio>:1250/?action=getdevices
```

The response contains a `DeviceArray` with one entry per device. Fields seen in a published example include `BootloaderRev`, `CANbus`, `CurrentVers`, `HardwareRev`, `ID`, `ManDate`, `Model`, `Name`, and `SerialNo` (RomanTechPlus, 2022).

> **Verify first:** That example dates from the Phoenix 5 era (2022). Open the URL in a browser against your current robot before relying on the field names.

### Minimal Java sketch

Run this in a background thread at startup, retrying until the devices enumerate:

```java
var client = HttpClient.newHttpClient();
var req = HttpRequest.newBuilder(
    URI.create("http://localhost:1250/?action=getdevices")).build();
String json = client.send(req, HttpResponse.BodyHandlers.ofString()).body();
SignalLogger.writeString("CANInventory/raw", json);   // and/or DataLogManager.log(json)
```

Then parse `DeviceArray` and write one entry per device:

```
{Model}-{CANbus}-{ID}  ->  {SerialNo, CurrentVers}
```

Each log then carries a map from CAN ID to physical unit.

### Practical notes

- Run the call once while disabled, not every loop. It triggers a bus enumeration.
- Don't block `robotInit`. Devices can take a few seconds to appear after boot.
- CANivore devices come back in the same array, tagged by their `CANbus` field.

## Why It's Worth Doing

Logging the serial lets you track wear and failures per physical motor across rebuilds, and even across robots, instead of per CAN ID.

## References

APA 7th edition. Full list: [references.md](references.md).

CTR Electronics. (n.d.-e). *Device details*. Phoenix 6 Documentation, Tuner X. Retrieved October 4, 2026, from https://v6.docs.ctr-electronics.com/en/latest/docs/tuner/device-details-page.html

CTR Electronics. (n.d.-f). *Connecting Tuner*. Phoenix 6 Documentation, Tuner X. Retrieved October 4, 2026, from https://v6.docs.ctr-electronics.com/en/stable/docs/tuner/connecting.html

CTR Electronics. (n.d.-g). *phoenix6.hardware.traits.common_device*. Phoenix 6 Python API Reference. Retrieved October 4, 2026, from https://api.ctr-electronics.com/phoenix6/stable/python/autoapi/phoenix6/hardware/traits/common_device/index.html

CTR Electronics. (n.d.-h). *Prepare the robot controller* (Phoenix 5 documentation, ch. 6) [Source documentation]. GitHub. Retrieved October 4, 2026, from https://github.com/CrossTheRoadElec/Phoenix-Documentation/blob/master/source/ch06_PrepRobot.rst

RomanTechPlus. (2022, August 1). *Phoenix diagnostic serve questions* [Online forum post]. Chief Delphi. https://www.chiefdelphi.com/t/phoenix-diagnostic-serve-questions/413857
