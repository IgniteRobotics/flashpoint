## ADDED Requirements

### Requirement: First-run smoke on every platform
A first-run smoke job SHALL run on Linux, macOS and Windows for every release tag, once a night on the default branch, and on pull requests that change the package source, its packaging, or the smoke job itself. On each platform it SHALL, as a new team would:
1. build the package and install it into an empty environment;
2. create a configuration non-interactively for team 9999 with a new lake folder;
3. ingest one golden-corpus match with its wpilog and hoots, downloading the hoot converter over the network and verifying its checksum;
4. build Replay data;
5. start the app, check that its main page and the setup checks API both answer successfully, and stop it.

Each step SHALL report its own failure by name, with that step's output. The job SHALL NOT be part of the required check, so a flaky run doesn't block merges. For a release tag, the smoke SHALL install the package the release built (not a rebuild), and publishing to the package index SHALL wait until the smoke has passed on all three platforms; a failure on any platform SHALL hold the publish (see public-release, "A tag publishes the package and a GitHub Release").

#### Scenario: Smoke failure holds a release
- **WHEN** a version tag is pushed and the smoke fails on Windows
- **THEN** nothing is published to the package index, and the release run names the failed platform and step

#### Scenario: Release smoke tests the built package
- **WHEN** a version tag is pushed
- **THEN** each platform's smoke installs the package file produced by that release run's build, and its `flashpoint --version` matches the tag

#### Scenario: Smoke failure doesn't block a merge
- **WHEN** a pull request passes every blocking job and the smoke job fails on Windows
- **THEN** the required check passes, and the smoke failure is shown on the pull request

#### Scenario: Converter checksum mismatch
- **WHEN** the smoke run downloads a hoot converter whose checksum doesn't match the manifest
- **THEN** the hoots end `failed` with reason `owlet-checksum-mismatch` (not quarantined), the ingest step fails with that output, and the job fails

#### Scenario: Server doesn't answer
- **WHEN** the app doesn't answer on its address within the step's time limit
- **THEN** the serve step fails with the server's output, and the server process is stopped

#### Scenario: Docs-only change
- **WHEN** a pull request changes only documentation
- **THEN** the smoke job doesn't run
