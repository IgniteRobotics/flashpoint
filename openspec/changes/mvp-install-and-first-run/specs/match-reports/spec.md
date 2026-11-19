## ADDED Requirements

### Requirement: Empty Replay explains what to do
When Replay has no match to list and no match was asked for in the link, the view SHALL show the one state below that fits the lake. It SHALL suggest `flashpoint report` only in the third state.

1. **No sessions:** the lake holds no imported session. The view SHALL say so, name the lake folder, and give the import command. In served mode it SHALL link to the Setup view, with the number of setup checks reporting a problem when there are any. When the lake has quarantined files, it SHALL say how many and name `flashpoint doctor`.
2. **No match key:** the lake holds sessions, but none has a match key. The view SHALL say that Replay lists only competition matches (identified from FMS data in the log or a match-style log filename) and that the lake's N sessions are practice, pit or other non-match sessions. It SHALL link to History, where their usage counts, and point to the guide's section on how matches are identified (see team-setup, "Documentation links").
3. **Not built:** sessions with a match key exist, but no Replay data has been built for them. The view SHALL say how many matches are waiting and give `flashpoint report`.

The counts SHALL come from the lake's recorded state and SHALL be available both when Replay data was built with zero matches and when it was never built. A static export SHALL show the same states, without the Setup link and with the guide named by title.

#### Scenario: Blank lake
- **WHEN** `flashpoint serve` runs on an empty lake with no configuration
- **THEN** Replay shows the no-sessions state with the lake folder, `flashpoint ingest`, and a link to Setup naming the number of setup problems, and does not mention `flashpoint report`

#### Scenario: Practice logs only
- **WHEN** the lake holds only corpus `2025-nofms` (a wpilog with no FMS data and no match in its filename) and Replay data has been built
- **THEN** Replay shows the no-match-key state saying 1 session is not a competition match, links to History, points to the match identification section, and does not mention `flashpoint report`

#### Scenario: Keyed but not built
- **WHEN** corpus `2026-gacmp-q7` is ingested with `flashpoint ingest --no-report` and the app is served with `--no-build`
- **THEN** Replay shows the not-built state saying 1 match is waiting, with `flashpoint report`

#### Scenario: Only a quarantined file
- **WHEN** the lake's only file is the quarantined zero-byte corpus hoot `2026-gadal-q11-empty`
- **THEN** Replay shows the no-sessions state, says 1 file was quarantined, and names `flashpoint doctor`

#### Scenario: Static export of practice logs
- **WHEN** the practice-only lake above is exported as a static Replay and opened from disk with networking disabled
- **THEN** it shows the no-match-key state, names the guide section by title, shows no Setup link, and requests no address

## MODIFIED Requirements

### Requirement: Partial and unmatched sessions
Matches that cannot be fully shown SHALL still appear in the match list with a reason; they SHALL never be silently dropped:
- a match with low alignment confidence SHALL carry a visible warning;
- a session with no derived samples SHALL be listed with "no aligned samples" and its raw-log downloads only, with a reason that names the cause:
  - no robot is configured for the session's robot code: the reason SHALL name the project and season, and SHALL point to Draft robot in the Setup view (a link in served mode, named in a static export);
  - the session has hoots but no wpilog: the reason SHALL say there is no wpilog to align its hoots to;
  - otherwise, the reason SHALL say no device data could be aligned to the wpilog;
- a session with no match key SHALL NOT get a Replay entry, and the build summary SHALL count it.

#### Scenario: Low-confidence match
- **WHEN** a session aligned by enable edges is shown
- **THEN** its Replay header shows a low-alignment warning, and its match-list entry is marked

#### Scenario: Hoot-only session
- **WHEN** the lake holds a match-keyed session with hoots but no wpilog
- **THEN** the match list shows it with "no aligned samples", its reason says there is no wpilog to align its hoots to, and selecting it offers only the raw downloads

#### Scenario: Match with no robot
- **WHEN** corpus `2026-gacmp-q7` is ingested with `--no-robot-draft` and no robot file, and Replay is served
- **THEN** Q7 is listed with "no aligned samples", its reason names project `Robot-2026` and season 2026 and links to Draft robot, and it does not mention hoots without a wpilog

