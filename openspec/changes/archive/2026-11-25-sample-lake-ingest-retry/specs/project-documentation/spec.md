## ADDED Requirements

### Requirement: Sample lake rides out a failed file
When the media command builds a sample lake, it SHALL judge each ingest run by that run's own counts of failed and quarantined files, not by its exit code alone.

- If the run reports one or more **failed** files and no **quarantined** files, the command SHALL run the same ingest again, up to 2 more times. Before each retry it SHALL print the files it is retrying and the run number.
- If any file is still failed after the last run, the command SHALL exit non-zero and name each such file and the number of runs.
- If the run quarantines any file, the command SHALL NOT retry it. It judges the run's exit code as it does today: a lake that expects a quarantine accepts it, and any other lake fails, naming the quarantined file.
- If the run exits non-zero with no failed and no quarantined files, or the command can't read the run's summary, the command SHALL NOT retry. It SHALL fail as it does today, printing the run's output.
- When a retry succeeds, the command SHALL say that it retried and which files.

This applies to every sample lake the command builds. The Replay build step that follows ingest is not retried.

#### Scenario: A file fails once, then imports
- **WHEN** a sample lake is built, the first ingest run reports one file failed and nothing quarantined, and the second run imports it
- **THEN** the build completes, and its output names the file and says it was retried

#### Scenario: A file keeps failing
- **WHEN** a sample lake is built and the same file is reported failed on the first run and on both retries
- **THEN** the command exits non-zero after 3 ingest runs and names the file and the 3 runs

#### Scenario: A quarantined file is not retried
- **WHEN** a sample lake that doesn't expect a quarantine is built and the first ingest run quarantines a file
- **THEN** ingest runs only once, and the command exits non-zero and names the quarantined file

#### Scenario: A retry that quarantines the file still fails
- **WHEN** the first ingest run reports a file failed, and the retry quarantines that file
- **THEN** the command exits non-zero and names the quarantined file, and does not retry again

#### Scenario: The quarantine sample lake still builds
- **WHEN** the quarantine sample lake is built from the corpus's empty hoot
- **THEN** ingest runs once, quarantines the empty hoot, and the build continues

#### Scenario: A failure that isn't a file is not retried
- **WHEN** an ingest run exits non-zero and reports no failed and no quarantined files (for example, the derive after it failed)
- **THEN** ingest runs only once, and the command exits non-zero and prints that run's output

#### Scenario: Ingest summary wording changes
- **WHEN** the wording of `flashpoint ingest`'s per-file FAILED line or its count line changes
- **THEN** the unit test run fails, before any docs deploy depends on the old wording
