# View: Live CAN

**Status: experimental.** Live CAN needs robot code that publishes CAN health ([Adding it to robot code](../../how-to/live-can-health.md#adding-it-to-robot-code)), and it can report false dropouts on fast buses: on our robot's CANivore, with signals at 250 Hz, gaps under 15 ms were counted as dropouts. Until a per-bus dropout window ([#66](https://github.com/IgniteRobotics/flashpoint/issues/66)) lands and the pit check on a real robot passes, treat a lone dropout on a fast bus with suspicion and confirm it after the match in Replay's CAN markers. The Live CAN view reads the robot while you work on it, and shows which CTRE device dropped out, rebooted or faulted, and whether it was one device or the whole bus. Use it for a shake test (robot disabled, wiggle each harness) or a drive test (robot enabled). It records nothing: the lake and the configuration don't change.

![Live CAN view: CAN devices with dropouts, reboots and faults during a shake test](../../assets/media/live-can.png)

How to set up the robot code and run a test is in [Live CAN health](../../how-to/live-can-health.md). This page describes what's on the screen.

## Layout

| Part | What it holds |
|---|---|
| Band | **LIVE CAN**, the connection pill, the enabled pill, the robot line, and **MARK** |
| Message | What's wrong with the connection, when something is |
| Bus panels | One per CAN bus: its counts, then a card per device |
| **EVENTS** | The event log, newest first |

There are no filters on this view. It needs `flashpoint serve`; a static export doesn't have it.

## Band

### Connection pill

| Pill | Meaning |
|---|---|
| **CONNECTING** | Looking for the robot. The message says "Looking for the robot. Turn it on and join its network; this keeps trying." |
| **WAITING FOR DATA** | Connected less than a second ago, and robot code hasn't published its version yet |
| **CONNECTED** | Connected, and robot code publishes live CAN health this Flashpoint can read |
| **NOT PUBLISHING** | Connected, but robot code doesn't publish live CAN health |
| **UNSUPPORTED VERSION** | Robot code publishes a contract version this Flashpoint doesn't support |

Flashpoint connects to the robot from the laptop running `serve`, at the addresses `flashpoint acquire` uses (from your team number, or `hosts` in [acquire.toml](../config/acquire.md)), or the ones given with `flashpoint serve --robot-host` ([`flashpoint serve`](../cli.md#flashpoint-serve)). It connects only while a Live CAN view is open somewhere, and disconnects about 30 seconds after the last one closes.

### Enabled pill and robot line

- **ENABLED** or **DISABLED**, as robot code reports it, or a dash before it has.
- The robot line: "robot _address_" once connected ("trying _addresses_" before), "project _name_" from robot code, and "config _robot_", the robot file whose `project` matches.

### MARK

Type what you're about to do in **MARK** ("now wiggling: FL module"), at most 60 characters, and press Enter or **SET**. Every event after that carries the mark, until you set another or press **CLEAR**. Beside it: "marking: _mark_", or "no mark".

## Bus panels

Each bus has a heading with its name (as robot code names it, such as `rio` or `DriveTrain`), then:

- **BUS DROPPED**, for 5 seconds after a bus-wide dropout;
- "_n_ reporting", and "· _m_ missing" when devices in the robot file aren't reported;
- **UTILISATION**, the share of the bus in use;
- **BUS OFF**, **TX FULL**, **RX ERRORS** and **TX ERRORS**, robot code's counts for the bus;
- **BUS-WIDE DROPS**, how many bus-wide dropouts the bus has had since Flashpoint connected.

### Device cards

A card per device, slots first (by slot id), then unmapped devices. Each shows its slot (or "unmapped"), its state, the slot's role, the device name (such as `TalonFX-11`), its dropout and reset totals since Flashpoint connected ("_n_ drops", "_n_ resets"), its lowest supply voltage in the latest update, and any fault names (under-voltage, boot during enable, hardware, bridge brownout, bad magnet).

A card shows one state, the first that applies:

| State | Meaning |
|---|---|
| DISCONNECTED | Robot code says it isn't heard at all |
| DROPPED | Had an isolated dropout in the last 5 seconds |
| REBOOTED | Reset in the last 5 seconds |
| FAULTED | A fault bit is set in its latest update |
| OK | Heard, nothing new |
| UNMAPPED | Reported, but no slot in the robot file matches its bus, model and CAN id |
| MISSING | In the robot file, but robot code doesn't report it: check it's wired, powered, and registered in robot code. Its card says "configured, not reported" |

DISCONNECTED, DROPPED and REBOOTED use the FAULT colour, FAULTED the WARN colour, and MISSING and UNMAPPED the NO DATA grey.

### Isolated or bus-wide

A dropout is **bus-wide** when at least half of the bus's other reporting devices also dropped in the same update. It's one **BUS DROP** event, counted in **BUS-WIDE DROPS** and not against any device. Otherwise it's isolated: a **DROPOUT** event, charged to that device. Updates arrive about every 100 ms, so a dropout that spans two updates can show as two isolated dropouts.

## EVENTS

One line per event, newest first, up to 500. Each line has the time (to the millisecond), **ENABLED** or **DISABLED**, what happened, the mark if one was set, and the device or bus:

| Event | What it shows |
|---|---|
| **DROPOUT** | _slot_ (_device_) on _bus_, "×_n_" dropouts, and the oldest signal age in ms |
| **BUS DROP** | _bus_: every device that dropped, how many, and the oldest age |
| **REBOOT** | _slot_ (_device_) on _bus_, "×_n_" resets |
| **FAULT** | _slot_ (_device_) on _bus_, and the fault's name |
| **LOST** | _slot_ (_device_) on _bus_, "not heard" |
| **BACK** | _slot_ (_device_) on _bus_, "heard again" |
| **BUS ERR** | _bus_, and which counts rose, such as "+2 rx errors" |
| **RESTART** | "robot code restarted", "totals start again" |

Robot code counts from when it started. When its totals go down, Flashpoint takes it as a robot code restart: one **RESTART** event, and counting starts again without reporting the drop as dropouts or reboots.

Before the first event: "No events yet. They appear here as the robot reports them."

## Messages

| Message | When |
|---|---|
| "No robot address. Run `flashpoint init --team <number>`, or start the server with `flashpoint serve --robot-host <address>`." | There's no team number and no address to try |
| "Looking for the robot. Turn it on and join its network; this keeps trying." | Not connected yet |
| "The robot is connected, but its code is not publishing live CAN health under `/Flashpoint/Live/CANHealth/`. To add it to robot code, see the Live CAN health guide, "Adding it to robot code"." | **NOT PUBLISHING** |
| "The robot publishes live CAN health version _n_; this Flashpoint supports version _m_. Update Flashpoint or the robot code so they match." | **UNSUPPORTED VERSION**. Nothing else shows |
| "No robot configuration has project _name_, so every device shows unmapped. Add or fix a robot file in Setup." | No robot file has the `project` robot code publishes ([robot files](../config/robots.md)) |
| "Ignored:" and the topics, each with why (such as "wrong type: string" or "negative count: -1") | Robot code publishes a topic with a bad value. The rest keeps working |

## Shared on the network

Live CAN changes nothing, so it works the same from another computer (`flashpoint serve --host 0.0.0.0`). The laptop running `serve` holds the one connection to the robot. Device totals come from it, so everyone watching sees the same totals, and they survive a reload. Marks and the event log stay in each browser: reloading the page clears them, and other people watching don't see yours.
