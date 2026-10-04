# wpilog-reading Specification

## Purpose
Decode WPILib DataLog (.wpilog) files, version 1.0, into typed, long-format samples. The decoding must be faithful to the file and fast enough to process a full match well within the ingest budget.

## Requirements

### Requirement: Faithful record decoding
The reader SHALL decode every record per the WPILib DataLog 1.0 specification. That includes Start, Finish, and Set-metadata control records, and data records for every entry. Its decoded record count and values SHALL match the official WPILib reader exactly.

#### Scenario: Matches the reference reader
- **WHEN** corpus `2026-gacmp-q7`'s wpilog is decoded
- **THEN** the number of data records, the set of entry names and types, and every scalar value equal the official WPILib reader's output

#### Scenario: Unordered timestamps
- **WHEN** records appear out of timestamp order (the format allows this)
- **THEN** every sample is kept with its original timestamp, and the output is sortable by entry and timestamp

### Requirement: Typed values
Scalar types (`double`, `float`, `int64`, `boolean`) SHALL be stored as typed numeric or boolean values, never as text. `string` and `json` SHALL be stored as text. Array, struct, struct-array, protobuf, and unknown types SHALL be preserved losslessly as raw bytes, together with their declared type name and any schema found in the log.

#### Scenario: Struct entry preserved
- **WHEN** a log contains `struct:Pose2d` entries and their `structschema`
- **THEN** each sample keeps its raw bytes and type name, and the schema text is retrievable for later decoding

### Requirement: Malformed input handling
The reader SHALL reject files whose header is invalid or whose version is unsupported. It SHALL stop cleanly at a truncated final record and report the number of trailing bytes it ignored.

#### Scenario: Truncated tail
- **WHEN** a wpilog's last record is cut off partway
- **THEN** every complete record is returned, and the result reports `truncated_bytes > 0`

### Requirement: Throughput
On the reference pit laptop (Apple Silicon or a recent x86-64), decoding SHALL run at no less than 10 million records per second, excluding the first-run compile warm-up.

#### Scenario: Large hoot export
- **WHEN** the all-signals wpilog export of the `2026-gacmp-q7` CANivore hoot (about 37 million records) is decoded
- **THEN** decoding finishes in under 4 seconds
