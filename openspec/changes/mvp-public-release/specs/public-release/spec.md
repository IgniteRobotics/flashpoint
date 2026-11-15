## Purpose

What a tagged Flashpoint release produces and what an outside team finds when it comes to the repository for help: the published package, a GitHub Release with that version's notes, documentation that matches the release, a written release checklist, and the issue forms, discussion space and community files that route their questions and reports.

## ADDED Requirements

### Requirement: A tag publishes the package and a GitHub Release
Pushing a version tag (`v` followed by the version) SHALL publish that version to the public package index and SHALL then create a GitHub Release for the tag. The release's notes SHALL be that version's section of `CHANGELOG.md` (the lines under its `## <version>` heading, which may carry a ` - <date>` suffix, up to the next `## ` heading), word for word. A plain `X.Y.Z` version SHALL be marked as the latest release. A version with a pre-release suffix (for example `0.3.0rc1`) SHALL be marked as a pre-release and SHALL NOT be marked latest. If `CHANGELOG.md` has no section for the tagged version, or the section has no text, the release SHALL stop before anything is published and SHALL name the version. The GitHub Release SHALL be created only after the package index publish succeeded, so a publish that is held back (by the release gate on the cross-platform smoke, or by the maintainer not approving it) or fails leaves no GitHub Release. A manual dry run SHALL publish to the test package index only and SHALL NOT create a GitHub Release.

#### Scenario: Final release
- **WHEN** tag `v0.3.0` is pushed and `CHANGELOG.md` has a `## 0.3.0 - 2026-12-19` section
- **THEN** version 0.3.0 is on the package index, a GitHub Release `v0.3.0` exists whose notes equal that section, and it is the repository's latest release

#### Scenario: Notes stop at the next section
- **WHEN** the notes are taken from a synthetic changelog with `## Unreleased`, `## 0.3.0 - 2026-12-19` and `## 0.2.0 - 2026-11-13` sections, for version 0.3.0
- **THEN** the notes hold the 0.3.0 section's text only, with no `Unreleased` or 0.2.0 lines

#### Scenario: Missing changelog section
- **WHEN** tag `v0.3.1` is pushed and `CHANGELOG.md` has no `## 0.3.1` section
- **THEN** the release fails naming version 0.3.1, nothing is published to the package index, and no GitHub Release is created

#### Scenario: Empty or malformed changelog section
- **WHEN** the notes are taken from a synthetic changelog whose `## 0.3.0` heading is followed directly by `## 0.2.0`, or whose only 0.3.0 heading is `### 0.3.0` (wrong level)
- **THEN** the extraction fails naming version 0.3.0, and the release would publish nothing

#### Scenario: Pre-release tag
- **WHEN** tag `v0.3.0rc1` is pushed with a `## 0.3.0rc1` section
- **THEN** a GitHub pre-release is created, and the latest release is unchanged

#### Scenario: Publish held back
- **WHEN** tag `v0.3.0` is pushed and the cross-platform smoke fails on Windows, so the publish does not run
- **THEN** 0.3.0 is not on the package index and no GitHub Release `v0.3.0` exists

#### Scenario: Dry run
- **WHEN** the release is run by hand with version `0.3.0.dev1`
- **THEN** that version is on the test package index, and no GitHub Release or tag is created

### Requirement: Corpus releases are never the latest release
A release that carries golden-corpus files rather than the package SHALL NOT be marked as the repository's latest release, now or when a new corpus version is published. The repository's latest release SHALL always be the newest final package version once one exists.

#### Scenario: Latest after 0.2.0
- **WHEN** the repository's releases are listed after `v0.2.0` is released
- **THEN** the latest release is `v0.2.0`, and `corpus-v1` is listed but not marked latest

#### Scenario: New corpus version
- **WHEN** a `corpus-v2` release is published by following the developer guide
- **THEN** it is not marked latest, and the latest release is still the newest package version

### Requirement: Release build refuses moving-branch documentation links
Before anything is published, the release build SHALL check the built package's metadata and its README as the package index will show it. It SHALL fail, naming the file and line, if any link points at a branch of the source repository that moves (`blob/main`, `blob/development`, `tree/main` or `tree/development`), or if any image is given by a relative path. An image served from the documentation site SHALL use the documentation version path of the version being built (major.minor). A failed check SHALL publish nothing.

#### Scenario: Built package metadata
- **WHEN** the package built for version 0.3.0 is inspected
- **THEN** no link in its metadata or README points at `blob/main`, `blob/development`, `tree/main` or `tree/development`, and every README image from the documentation site is under its `/0.3/` path

#### Scenario: Moving-branch link refused
- **WHEN** a synthetic README with a link to `https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/user-guide.md` is built into a package
- **THEN** the build check fails, names the README line, and nothing is published

#### Scenario: Relative image link refused
- **WHEN** the README is built into a package with an image given by a relative path
- **THEN** the build check fails and names the README line

#### Scenario: Image from another version
- **WHEN** the package for version 0.3.0 is built from a README whose screenshot is under the documentation site's `/0.2/` path
- **THEN** the build check fails and names the README line

### Requirement: Release checklist
The developer guide SHALL hold a release checklist that a maintainer follows for every release, in order:
1. a pull request promoting `development` to `main`;
2. a test package index dry run from `main`;
3. the cross-platform smoke on the tag;
4. clean installs on macOS, Linux and Windows from the test index;
5. the tag, then approving the publish;
6. checking the GitHub Release and the latest-release marker;
7. the yank policy (when a published version is yanked, and the reason text);
8. the documentation deploy for the version.

It SHALL state that release-preparation pull requests reference their release issue without a closing keyword, because the default branch is `development` and a closing keyword there closes the issue before anything ships. It SHALL name the owner and date of the next planned patch release when one is scheduled.

#### Scenario: Checklist present and complete
- **WHEN** the developer guide's release section is read
- **THEN** it lists the eight steps in that order and the rule against closing keywords in release-preparation pull requests

#### Scenario: Release issue stays open through preparation
- **WHEN** a release-preparation pull request written from the checklist merges into `development`
- **THEN** the release issue is still open, and it is closed only after the version is on the package index

### Requirement: Issue forms for Flashpoint users
The repository SHALL carry its own issue forms, which replace the organisation's robot-code templates on this repository. A bug form SHALL require the `flashpoint --version` output, the operating system, the Python version, the `flashpoint doctor` output, the kinds of logs involved (wpilog, rio hoot, CANivore hoot, AdvantageKit), and whether the hoots are Phoenix Pro (yes, no, don't know). For an import problem it SHALL ask for the ingest output line that names the file's stage and reason (for example quarantined or `incomplete-read`). It SHALL warn not to attach match logs to a public issue and say how to share them privately. A feature form SHALL ask what the user wants to see and which view or command it concerns. Blank issues SHALL be off, and the issue chooser SHALL link questions to the discussion space.

#### Scenario: New issue chooser
- **WHEN** a signed-in visitor opens "New issue" on the repository
- **THEN** they see the Flashpoint bug and feature forms and a link to the Q&A discussions, and none of the organisation's robot-code templates

#### Scenario: Bug form without a version
- **WHEN** a visitor submits the bug form with the version field empty
- **THEN** the form refuses to submit and marks the version field as required

#### Scenario: Quarantined log report
- **WHEN** a visitor reports a log that was quarantined on ingest
- **THEN** the bug form asks for the ingest line with the stage and reason, and states that the log itself should be shared privately, not attached

#### Scenario: Forms checked in the test suite
- **WHEN** a required field is removed from the bug form file
- **THEN** the unit test run fails and names the form and the missing field

### Requirement: Questions have a public place
The repository SHALL have discussions turned on with a Q&A category, where an answer can be marked as accepted. The README and CONTRIBUTING SHALL point questions there and point bugs at the issue forms.

#### Scenario: Ask a question
- **WHEN** a visitor follows the question link from the issue chooser or the README
- **THEN** they reach the Q&A category and can open a question there

### Requirement: Community files
The repository SHALL have a CONTRIBUTING file, a SECURITY policy and a code of conduct at its root or in `.github/`.
- CONTRIBUTING SHALL say how outsiders can help (issues, logs shared privately, forks), that pull requests are accepted from collaborators only, and what response time to expect during build season.
- SECURITY SHALL say which versions get fixes (the latest minor release) and how to report privately; private vulnerability reporting SHALL be on.
- The repository's community profile SHALL show the contributing guide, code of conduct and issue templates as present, and its security tab SHALL show the policy.

#### Scenario: Community profile
- **WHEN** the repository's community profile is fetched
- **THEN** the contributing guide, code of conduct, issue templates, license and README are all present

#### Scenario: Private vulnerability report
- **WHEN** a visitor follows SECURITY's reporting instructions
- **THEN** they reach a private report form, and the report is not visible to the public

#### Scenario: Outside pull request expectations
- **WHEN** someone who isn't a collaborator reads CONTRIBUTING before opening a pull request
- **THEN** it tells them pull requests are collaborators-only, offers an issue or a fork instead, and gives the build-season response time
