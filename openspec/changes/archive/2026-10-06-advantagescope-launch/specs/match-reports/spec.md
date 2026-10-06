## MODIFIED Requirements

### Requirement: Open in AdvantageScope
Each match SHALL offer its raw wpilog and hoots as downloads, named so their match is recognisable, for opening in AdvantageScope. In served mode on the local machine, each match SHALL also offer one "Open in AdvantageScope" action that opens the match's logs in place (see `advantagescope-launch`). After the action, the page SHALL show the staged folder and the hoots to add with File › Insert log. When the launch is unavailable, the page SHALL show why and keep the downloads. The app SHALL NOT launch any application except through that guarded launch. Raw files SHALL NOT count toward the payload budget. A static export made without raw files SHALL say the files are not included and name the lake hash of each.

#### Scenario: Download Q7 logs
- **WHEN** the user downloads the raw logs of Q7
- **THEN** the Q7 wpilog and its rio and CANivore hoots download with names containing `2026gacmp_qm7`, and their hashes equal the ledger hashes

#### Scenario: Launch from Replay
- **WHEN** the user chooses "Open in AdvantageScope" on Q15 in a locally served app, with a stand-in program configured as AdvantageScope
- **THEN** the stand-in is started on the staged Q15 wpilog, and the page names the staged folder and lists the two hoots to insert

#### Scenario: Launch unavailable
- **WHEN** Replay is served on the pit network
- **THEN** no launch action is shown, the page says the launch is only available on the machine running Flashpoint, and the downloads work

#### Scenario: Static export
- **WHEN** a static export is opened from disk
- **THEN** no launch action is shown, and the downloads work

#### Scenario: Exported without raw files
- **WHEN** a static export is made with raw files excluded
- **THEN** the download area shows "not included" with each file's hash, and the rest of Replay works
