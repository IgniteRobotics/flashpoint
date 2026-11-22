## MODIFIED Requirements

### Requirement: README is an overview and entry point
The repository README is also the PyPI project page. It SHALL open with a one-line pitch written for any FRC team, not a dictionary definition and not a single-team framing. Its first screen SHALL hold:
- two or three screenshots, or one animation, of Replay, History and Anomalies;
- a "Works with / needs" block naming:
  - CTRE Phoenix 6 devices and `.hoot` logs;
  - WPILib `.wpilog` logs;
  - Phoenix Pro, which motor temperature needs;
  - the three supported operating systems;
  - a link to the support matrix.

The README SHALL include:
- a quickstart that goes from a fresh machine to a served app: one install command followed by at most three `flashpoint` commands;
- links to the hosted demo and to the sample-logs quickstart;
- links to the docs site's Why Flashpoint, Quickstart, Install, Robot side, Support matrix and Troubleshooting pages.

Every link and image in the README SHALL be an absolute URL. In the repository, every image SHALL be served from the docs site's `latest` version. In a released package's description, every image SHALL be served from that release's version path (major.minor), so each release's PyPI page keeps that release's screenshots. In a released package's description, no link SHALL point at a moving branch of the repository (`blob/main`, `tree/main`, `blob/development`, `tree/development`); links to repository files SHALL point at that release's tag. The README SHALL NOT be organized by roadmap phase.

#### Scenario: New reader finds the purpose and the guides
- **WHEN** a reader opens the README
- **THEN** the first section states what Flashpoint ingests, what it tracks, and that it runs offline on a pit laptop; the "Works with / needs" block names Phoenix 6, says temperature needs Phoenix Pro, and links the support matrix; and the README links the docs site's Why Flashpoint, Quickstart, Install, Robot side, Support matrix and Troubleshooting pages

#### Scenario: Images in the repository README
- **WHEN** the README's images are listed
- **THEN** each image is an `https://` URL under the docs site's `latest` path, naming a file that the media manifest produces

#### Scenario: Images render on PyPI
- **WHEN** the 0.3.0 wheel's long description is read
- **THEN** every image URL in it is under the docs site's `0.3` version path, and every link to a repository file points at `blob/v0.3.0` or `tree/v0.3.0`, never at a moving branch

#### Scenario: Pre-release images resolve
- **WHEN** the `0.3.0rc1` wheel's long description is read after its tag's docs deploy
- **THEN** every image URL in it is under the `0.3` version path and returns HTTP 200

#### Scenario: Relative image is refused
- **WHEN** the README contains a relative image path such as `media/replay.png`
- **THEN** the unit test run fails and names the README line

#### Scenario: Released images resolve
- **WHEN** the docs for a release tag have been deployed
- **THEN** every image URL in that release's README returns HTTP 200

#### Scenario: No phase-organized sections
- **WHEN** the README's headings are listed
- **THEN** none of them names a roadmap phase (P0 to P6)

#### Scenario: Short quickstart
- **WHEN** the commands in the README's quickstart block are counted
- **THEN** the first installs Flashpoint, at most three `flashpoint` commands follow it, and the last serves the app

### Requirement: User guide
The documentation SHALL include user documentation for people who run Flashpoint but don't change its code. It SHALL be spread across the docs site's Install, Quickstart, Tutorials, How-to guides and Reference sections. Together these SHALL cover:
- what Flashpoint is for, and what it isn't: it links to AdvantageScope for single-log deep dives rather than replacing it;
- installing on macOS, Linux and Windows, one page per OS:
  - the documented install brings its own supported Python;
  - the alternative install is also given;
  - the platform floor is stated;
  - updating is covered;
- ingesting logs by hand;
- serving the app and reading the Replay, History, Anomalies, Setup and Live CAN views, including the shared filters and anomaly triage;
- building a static Replay export for a USB stick;
- describing a robot in its config file;
- where the lake lives and how to point Flashpoint at another one;
- reading the output of `flashpoint doctor`.

#### Scenario: First run from the user guide
- **WHEN** a reader follows one OS's Install page and then the Quickstart's ingest and serve steps on that OS, using a match from the golden corpus
- **THEN** each command succeeds as written, and the served app lists that match in Replay

#### Scenario: Offline preparation is called out
- **WHEN** a reader looks for what to do before taking a laptop to an event
- **THEN** the docs say the hoot converter is downloaded on first use and must be fetched while online, give the command that does it, and link the offline install page

### Requirement: Automatic import guide
The documentation SHALL include a how-to guide to automatic log import as it exists. It SHALL cover:
- what the watch pulls (roboRIO internal and USB log roots, and removable volumes on the pit machine), and that it never modifies or deletes anything on the robot or the stick;
- every key in the acquisition config file, with its default;
- a dry run before installing a service;
- installing the service on Linux and Windows;
- running it on macOS, where no service definition exists;
- optional backup and the restore drill;
- checking status;
- troubleshooting for:
  - robot not reachable;
  - USB volume not seen;
  - file skipped as still being written;
  - low robot disk space;
  - a stale lock.

It SHALL state plainly that importing from cloud folders is not supported. While the release labels pulling from the robot with a maturity label (such as "beta"), the guide SHALL show the same label at the top of the robot-pull section.

#### Scenario: Linux service from the guide
- **WHEN** a reader follows the Linux steps on a machine with a user service manager
- **THEN** the unit file the guide names exists in the repository, and the commands enable it and show its log

#### Scenario: macOS limitation is explicit
- **WHEN** a macOS reader looks for how to run import in the background
- **THEN** the guide says there's no service definition for macOS, and gives the foreground command to run instead

#### Scenario: Install command is correct
- **WHEN** a reader runs the install command the guide gives for a service install
- **THEN** it is the same command the Install page gives, and it installs the published Flashpoint package whose version matches the docs version being read

#### Scenario: Beta label matches the app
- **WHEN** `acquire --help` labels robot pull "beta"
- **THEN** the guide's robot-pull section starts with the same "beta" label

### Requirement: CLI reference
The documentation SHALL include a CLI reference with one section for every `flashpoint` subcommand. Each section SHALL give:
- the subcommand's purpose;
- an options table generated from the CLI's own definitions, listing every option the subcommand accepts, with its help text and its default;
- at least one example.

Every option SHALL have non-empty help text. Generating the tables SHALL be one command, and a generated table that no longer matches the CLI SHALL fail the unit test run.

#### Scenario: Every subcommand documented
- **WHEN** the set of subcommands the CLI accepts is compared with the section headings in the CLI reference
- **THEN** each subcommand has a section

#### Scenario: New option without regeneration
- **WHEN** an option is added to a subcommand and the reference isn't regenerated
- **THEN** the unit test run fails and names the subcommand and the option

#### Scenario: Option without help text
- **WHEN** a subcommand gains an option with no help text
- **THEN** generating the reference fails and names the subcommand and the option

### Requirement: Documentation drift checks
The unit test suite SHALL check that:
- every relative link resolves to a file that exists in the repository, in the README, the deploy README, and every page of the docs site source, including pages in subfolders;
- every docs site page is listed in the site navigation;
- every docs link the CLI or the app shows resolves to a docs site page;
- every `flashpoint` command line in a docs code block is accepted by the CLI;
- every key each config file accepts appears in that file's reference page;
- every view in the app's navigation has a reference page and a screenshot in the media manifest;
- every Java code block in the robot-side pages appears unchanged in a file under `examples/robot-java/`;
- every message the troubleshooting page quotes as exact output still appears in the code.

A failed check SHALL name the file, the line, and the broken item. The checks SHALL run on every pull request on Linux and Windows, and SHALL NOT need the network or the golden corpus.

A strict build of the docs site SHALL also run on every pull request, without the corpus, using placeholder images named by the media manifest. It SHALL fail on a broken internal link, a broken anchor, or a missing image.

#### Scenario: Broken link fails the suite
- **WHEN** a guide contains a relative link to a file that doesn't exist
- **THEN** the unit test run fails and names the guide, the line number, and the link target

#### Scenario: Broken link in a nested page
- **WHEN** a page in a subfolder of the docs site source links to a sibling page that doesn't exist
- **THEN** the unit test run fails and names that page, the line, and the target

#### Scenario: Undocumented subcommand fails the suite
- **WHEN** a new subcommand is added to the CLI without a section in the CLI reference
- **THEN** the unit test run fails and names the subcommand

#### Scenario: Command example that the CLI rejects
- **WHEN** a how-to shows `flashpoint ingest --recursive logs/` and `ingest` has no `--recursive` option
- **THEN** the unit test run fails and names the page, the line, and the rejected option

#### Scenario: Page left out of the navigation
- **WHEN** a new page is added to the docs site source but not to the navigation
- **THEN** the unit test run fails and names the page

#### Scenario: App links to a page that moved
- **WHEN** the app's backup warning links to a docs page path that no longer exists in the site source
- **THEN** the unit test run fails and names the path and where the code uses it

#### Scenario: Broken anchor fails the strict build
- **WHEN** a page links to `troubleshooting.md#no-temperature` and that page has no such heading
- **THEN** the strict site build fails and names the page and the anchor

#### Scenario: External links are not fetched
- **WHEN** a guide contains an absolute web link
- **THEN** the check skips it and makes no network request

#### Scenario: Malformed link syntax is reported, not crashed on
- **WHEN** a guide contains an unclosed link, such as `[text](path` with no closing parenthesis
- **THEN** the check reports that file and line as malformed instead of raising an unhandled error

## ADDED Requirements

### Requirement: Published, versioned docs site
The documentation SHALL be published at `https://igniterobotics.github.io/flashpoint/`, with:
- one version for each released minor version (such as `0.3`);
- an alias `latest` for the newest release;
- a `dev` version built from the default branch.

The site root SHALL open `latest`. Every page SHALL show which version it describes and offer a version selector. Every `dev` page SHALL carry a banner saying it describes unreleased code, with a link to `latest`.

Deploys:
- A release tag `vX.Y.Z` SHALL deploy version `X.Y`. A patch release replaces that version's content.
- A pre-release tag (such as `v0.3.0rc1`) SHALL deploy version `X.Y` with a banner saying it describes a pre-release, so that pre-release's package page images resolve. It SHALL NOT move `latest`. The final `vX.Y.0` tag replaces it.
- The tag SHALL move `latest` only when it is the highest final release.
- Other versions SHALL stay as they were.
- A deploy whose strict build fails SHALL publish nothing.

#### Scenario: Tag deploys matching docs
- **WHEN** tag `v0.3.0` is pushed
- **THEN** `/0.3/` exists and `latest` points to it, and its CLI reference lists exactly the subcommands that the 0.3.0 package accepts

#### Scenario: Pre-release tag leaves latest alone
- **WHEN** tag `v0.3.0rc1` is pushed while `latest` is still aliased to `dev`
- **THEN** `/0.3/` exists with the pre-release banner on every page, and `latest` is unchanged

#### Scenario: Patch release replaces its minor version
- **WHEN** tag `v0.3.1` is pushed after `v0.3.0`
- **THEN** `/0.3/` shows the 0.3.1 content, and every other published version is unchanged

#### Scenario: Unreleased docs are labelled
- **WHEN** a commit lands on the default branch
- **THEN** `/dev/` is rebuilt, and each of its pages shows the "unreleased" banner linking to `latest`

#### Scenario: A failed build publishes nothing
- **WHEN** a page has a broken internal link at tag time
- **THEN** the deploy fails, and the previously published versions and `latest` are unchanged

### Requirement: Offline copy of the docs
Each release SHALL provide that version of the site, with its images, as one archive. Unpacked, it SHALL open from disk with no network. The offline install page SHALL say where to get it.

#### Scenario: Docs at a pit with no internet
- **WHEN** the release's docs archive is unpacked on a machine with networking disabled, and its start page is opened in a browser
- **THEN** the pages, navigation, internal links and images all work

### Requirement: Documentation structure
The docs site navigation SHALL have these top-level sections, in this order:
1. Why Flashpoint
2. Quickstart
3. Install, with one page each for macOS, Linux and Windows
4. Tutorials
5. How-to guides
6. Robot side
7. Reference: CLI, config files, views
8. Support matrix and known limits
9. Troubleshooting and FAQ
10. Under the hood: how it works, developer guide
11. Contributing

The hosted demo page sits under Quickstart, the privacy page under Reference, and the FAQ under Troubleshooting and FAQ.

Every page SHALL be one kind: tutorial, how-to, reference or explanation. Every how-to page SHALL have a title that names the task, and these parts:
- "Before you start", with what's needed;
- numbered steps;
- "Check it worked", with the output or screen to expect;
- a link to the matching troubleshooting entries.

The how-to guides SHALL include at least:
- set up a robot file (draft it from a log);
- import logs from a USB stick;
- import logs from the robot automatically;
- back up the lake and run the restore drill;
- move Flashpoint to another laptop;
- start a new season;
- take Replay on a USB stick;
- show the app to others on the pit network;
- watch live CAN health during a shake test;
- triage an anomaly;
- open a match in AdvantageScope;
- share logs privately for a bug report;
- update Flashpoint;
- install without internet;
- set the team name and colours.

#### Scenario: Navigation order
- **WHEN** the built site's top-level navigation is listed
- **THEN** it shows the sections above, in that order

#### Scenario: How-to shape
- **WHEN** any how-to page is read
- **THEN** it has "Before you start", numbered steps, "Check it worked", and a link into Troubleshooting

#### Scenario: Required how-tos exist
- **WHEN** the How-to guides section is listed
- **THEN** each task in the list above has its own page

### Requirement: Why Flashpoint page
The docs SHALL have a "Why Flashpoint" page that pitches Flashpoint to an FRC mentor or student in FRC terms: the motor that died in eliminations, the CAN dropout nobody could find, and the brownout in autonomous. It SHALL say what sets Flashpoint apart:
- it follows each physical motor by serial number through swaps, when the robot logs its CAN inventory (Robot side), and by CAN slot otherwise;
- it keeps a lifetime history across matches, events and seasons;
- it flags anomalies with evidence, and lets the team triage them;
- it runs offline on a pit laptop, with no account and no telemetry;
- it reads logs that teams already record;
- it is free and open source;
- it opens AdvantageScope for single-log deep dives instead of replacing it.

Every claim SHALL be followed by a screenshot or animation, or by a link to the page that shows it. The page SHALL have a "What it doesn't do (yet)" section that links the known limits, and SHALL link the hosted demo and the Quickstart. It SHALL make no claim about a device or log setup that the support matrix marks unsupported or not tested.

#### Scenario: Every claim has evidence
- **WHEN** the page's claims are listed
- **THEN** each is followed by an image from the media manifest, or by a link to a docs page

#### Scenario: Limits are one click away
- **WHEN** a reader looks for what Flashpoint can't do
- **THEN** the page's "What it doesn't do (yet)" section links the support matrix and known limits

#### Scenario: No claim beyond the matrix
- **WHEN** the page is compared with the support matrix
- **THEN** it doesn't present any device or log setup as working that the matrix marks unsupported or not tested

### Requirement: Quickstart with sample logs
The Quickstart SHALL give the install and the commands that ingest a folder of the reader's own logs and serve the app. It SHALL also have a "Try it with sample logs" path.

The sample bundle:
- is published with the docs;
- holds one corpus-v1 match (its wpilog and hoots) and a robot file for that robot;
- comes with its size and SHA-256.

The path's commands:
- check the bundle's SHA-256;
- ingest the bundle into a separate sample lake, with the bundle's own configuration, so the reader's own configuration and lake are untouched;
- are given for macOS and Linux shells, and for Windows PowerShell.

The sample match SHALL be a full match: the robot enabled through autonomous and teleop, with its wpilog and hoots lined up, so the first-event tutorial can follow a motor in History and triage an anomaly from it. The sample's robot file declares signals that the match's robot code didn't log yet, so ingest notes them; the Quickstart SHALL say this note is expected for the sample. The Quickstart SHALL end by linking the Robot side section.

#### Scenario: Sample logs reach Replay
- **WHEN** a reader on a clean machine follows "Try it with sample logs"
- **THEN** Replay lists the sample match with motor tracks, and the reader's own config folder and default lake are unchanged

#### Scenario: Sample is a full match
- **WHEN** the bundle is ingested by the documented commands
- **THEN** ingest succeeds with no file warnings, the sample match has autonomous and teleop phases, detection finds at least one anomaly in it, and the Quickstart's note about the declared-signals note quotes the same words

#### Scenario: Damaged download
- **WHEN** a copy of the bundle cut short in download is checked with the documented SHA-256 command
- **THEN** the check reports a mismatch, and the Troubleshooting page has an entry for it with "download it again" as the fix

### Requirement: First-event tutorial
The docs SHALL have a tutorial that takes a new reader through a first event end to end using the sample bundle:
1. copy logs from a USB stick (a folder stands in for one);
2. ingest them;
3. find the hottest motor in Replay;
4. follow that motor in History;
5. open Anomalies and triage one finding;
6. write a static Replay export for a USB stick.

Each step SHALL show the screenshot of the screen the reader should reach.

#### Scenario: Tutorial runs as written
- **WHEN** a reader follows the tutorial on a clean install with the sample bundle
- **THEN** each command succeeds as written, and each step reaches the view and state its screenshot's manifest entry names

### Requirement: Config and views reference
The Reference section SHALL have:
- one page for each config file a team edits: acquisition, theme, report, anomaly settings, anomaly causes, failure labels, season files, robot files and the units list. Each page lists every key the file accepts, with its type, default and meaning, and a "Common errors" part that quotes the message a bad value gives.
- one page for each view in the app's navigation: Replay, History, Anomalies, Setup and Live CAN. Each page has the view's current screenshot and explains each control, marker and badge on it.

#### Scenario: New config key without docs
- **WHEN** a key is added to the robot file's accepted keys without being added to the robot file reference
- **THEN** the unit test run fails and names the file and the key

#### Scenario: Bad key message is documented
- **WHEN** a robot file with an unknown key is loaded
- **THEN** the message printed matches the one the robot file reference quotes under "Common errors"

### Requirement: Generated screenshots and animations
One command SHALL produce every docs image and animation:
1. it builds a sample lake in a temporary folder from fetched corpus-v1 matches;
2. it serves the lake on loopback;
3. it captures every entry in the media manifest.

The manifest SHALL include:
- a screenshot of each view in the app's navigation (Live CAN driven by the built-in fake robot);
- animations of the main flows: ingest to Replay, picking a motor in History, triaging an anomaly, and Live CAN dropouts.

Capture conditions:
- the viewport, device scale, palette, colour scheme and clock are fixed;
- network access is blocked except loopback;
- given the same corpus and code, the same set of file names results.

Size limits: each screenshot at most 500 KB; each animation at most 3 MB and at most 20 seconds. Every manifest entry SHALL carry alt text, and every page that shows the image SHALL use it.

The output SHALL NOT be committed to the source branches, and SHALL NOT be included in the wheel or sdist.

The command SHALL fail with a non-zero exit, naming the cause:
- when a corpus file is missing (naming the file and the fetch command);
- when a manifest entry fails to capture;
- when a view logs a console error or shows an error state the entry doesn't expect.

#### Scenario: Every view captured
- **WHEN** the capture command runs against a fetched corpus-v1
- **THEN** it writes one screenshot for each view in the app's navigation and one animation for each manifest flow, each within its size limit

#### Scenario: Corpus not fetched
- **WHEN** the capture command runs with no corpus present
- **THEN** it exits non-zero, names the first missing corpus file and the fetch command, and writes no images

#### Scenario: Incomplete log is captured as such
- **WHEN** the sample lake is built from the E10 match, whose hoots the event's power-off cut short
- **THEN** capture completes, and the manifest's troubleshooting screenshot shows the app's incomplete-log marker on that match

#### Scenario: Damaged corpus files stay out of the sample lake
- **WHEN** the sample lake is built
- **THEN** it holds neither the corpus's empty hoot nor its truncated copy of the E10 rio hoot, so no sample match carries duplicated or partial copies of the same file

#### Scenario: No media in the package
- **WHEN** the wheel and sdist are built
- **THEN** neither contains a `.png`, `.gif`, `.webm` or `.mp4` file

#### Scenario: Image without alt text
- **WHEN** a media manifest entry has empty alt text
- **THEN** the unit test run fails and names the entry

#### Scenario: View without a manifest entry
- **WHEN** a view is added to the app's navigation without a media manifest entry
- **THEN** the unit test run fails and names the view

### Requirement: Hosted Replay demo
Each docs version SHALL publish a static Replay export of the sample matches (the corpus-v1 sample groups and the `samples-v1` matches), without raw logs, at `demo/` under that version's path. The README, the docs home and the Why page SHALL link the `latest` demo. The demo SHALL:
- open with no server;
- make no request to any host but its own;
- name itself as sample data in the app header;
- be at most 15 MB.

The docs page that links the demo SHALL say it is team 6829's robot, that it is a Replay-only static export, and link the Quickstart.

A deploy in which any sample match fails to build SHALL fail rather than publish a partial demo.

#### Scenario: Demo works with other hosts blocked
- **WHEN** the deployed demo is opened in a browser with requests to every other host blocked
- **THEN** Replay lists the sample matches, their tracks render, the header names the demo as sample data, and the console shows no errors

#### Scenario: Broken sample match stops the deploy
- **WHEN** a sample match fails to build during a docs deploy
- **THEN** the deploy fails, and the previously published demo stays in place

### Requirement: Robot-side "What to log" guide and samples
The Robot side section SHALL open with a ranked "What to log" checklist. Each row SHALL give:
- what to add;
- what Flashpoint gains;
- what you lose without it;
- the sample file that does it.

The rows SHALL cover, in rank order:
1. Phoenix 6 signal logging that writes hoots: where the hoots land, and checking for a hoot after each match;
2. the WPILib DataLog, with Driver Station data logged into it;
3. status-signal update rates for current, voltage, velocity and temperature, including the effect of CTRE's bus-utilisation optimisation;
4. Phoenix Pro for temperature;
5. build info;
6. the CAN inventory;
7. CAN bus status;
8. battery voltage and PDH. This row says that Flashpoint uses a battery proxy today. Any entry names it suggests are marked "planned, not read yet";
9. free disk space on the roboRIO and its USB stick.

The section SHALL also:
- say the samples are Java only;
- say which logged fields Flashpoint doesn't read yet (such as firmware);
- carry an AdvantageKit note that matches the support matrix.

Every sample SHALL be a file under `examples/robot-java/`. Each file SHALL have a "Tested with WPILib <version>, Phoenix 6 <version>" line. The folder SHALL build as a robot project.

#### Scenario: Samples compile
- **WHEN** the `examples/robot-java/` project is built with JDK 17 against the WPILib and Phoenix 6 versions its "Tested with" lines name
- **THEN** it compiles with no errors

#### Scenario: Sample without a tested-with line
- **WHEN** a Java file under `examples/robot-java/` has no "Tested with" line
- **THEN** the unit test run fails and names the file

#### Scenario: Snippet drifts from its file
- **WHEN** a Java block on a robot-side page differs from every region of the files under `examples/robot-java/`
- **THEN** the unit test run fails and names the page and line

#### Scenario: Inventory error behaves as documented
- **WHEN** a synthetic wpilog whose CAN inventory entry holds the documented error form is ingested and derived
- **THEN** Flashpoint records the error and keeps the log's history by swap dates, as the guide says

### Requirement: Support matrix and known limits
The docs SHALL have a support matrix with three tables.

**Device families.** Rows:
- Talon FX (Falcon 500, Kraken);
- Talon FXS;
- Pigeon 2;
- CANcoder;
- CANrange;
- CANdi;
- CANivore;
- Phoenix 5 devices;
- REV Spark MAX and Spark Flex;
- REV PDH;
- CTRE PDP.

Columns: Replay tracks, History and motor health, temperature, CAN health, Draft robot, Live CAN.

**Log setups.** Rows:
- Pro hoots with wpilog;
- non-Pro hoots with wpilog;
- wpilog only;
- hoots only;
- AdvantageKit wpilog with hoots;
- rio bus only.

Columns: match identity, clock alignment method, motor tracks, temperature, anomalies, Draft robot.

**Platforms.** Rows: macOS, Linux, Windows. Columns: install, ingest, serve, the automatic import service, Live CAN.

Each cell SHALL read one of:
- "supported", or "supported (needs Phoenix Pro)": the cell names its evidence (a corpus group, a test fixture, or a dated outside-team log check);
- "rough": works end to end with a named workaround (for example Windows, if its walkthrough finds one), naming the workaround and its issue;
- "not tested on real logs": only synthetic fixtures cover it;
- "not tested";
- "not supported": the row links to an issue that tracks it.

A Known limits section SHALL state the status of:
- the 2027 season and Systemcore;
- the first hoot needing internet for the converter;
- hoot-only sessions;
- each feature the release labels beta or experimental;
- the measured time, peak memory and lake size of a first ingest of a full event, when the release's measurement is over its target;
- each known issue the release ships with, linking its issue.

#### Scenario: Supported means tested
- **WHEN** each "supported" cell is checked
- **THEN** it names a corpus group, a test fixture, or a dated outside-log check that exists

#### Scenario: No outside logs
- **WHEN** no outside-team logs have been checked for a log setup
- **THEN** that row's cells read "not tested", or "not tested on real logs" where only synthetic fixtures cover it, and none reads "supported"

#### Scenario: Cell outside the vocabulary
- **WHEN** the matrix data gives a cell the value "works"
- **THEN** the unit test run fails and names the table, the row and the column

#### Scenario: Unsupported hardware is named
- **WHEN** a REV team reads the device table
- **THEN** the REV rows read "not supported" and link an open tracking issue

### Requirement: Troubleshooting and FAQ
The docs SHALL have a troubleshooting page for the first 30 minutes. Each entry SHALL give:
- the symptom;
- the exact message as printed;
- the cause;
- the fix.

It SHALL cover at least:
- the `flashpoint` command not found;
- the Python version being refused;
- the hoot converter download being blocked by a proxy;
- every log showing as non-match;
- an empty Replay, for each of its empty states;
- no temperature;
- the robot shown as "unknown";
- Windows paths and quoting;
- a quarantined or incomplete file;
- each `PROBLEM` line `doctor` can print.

An FAQ SHALL answer:
- REV;
- AdvantageKit;
- non-Pro hoots;
- Systemcore and 2027;
- Windows;
- cost;
- where the data lives;
- whether Flashpoint sends data anywhere;
- sharing one lake between laptops;
- how to get help, and what support to expect during build season.

#### Scenario: Quoted message matches the code
- **WHEN** the code changes a message that a troubleshooting entry quotes as exact output
- **THEN** the unit test run fails and names the entry

#### Scenario: Quarantine entry from a real file
- **WHEN** the corpus's empty hoot is ingested
- **THEN** ingest quarantines it, and the message printed is the one the quarantined-file entry quotes

#### Scenario: Incomplete entry from a real file
- **WHEN** the corpus's truncated copy of the E10 rio hoot is ingested
- **THEN** ingest succeeds with a warning, and the warning printed is the one the incomplete-file entry quotes

### Requirement: Offline install
The docs SHALL give a tested recipe, for each OS, for installing Flashpoint on a laptop with no internet:
1. on an online machine with the same OS and architecture, collect into one folder:
   - Flashpoint and every dependency for the target Python;
   - the hoot converter versions the team's logs need;
   - the offline docs;
2. install from that folder on the offline laptop.

#### Scenario: Install with networking disabled
- **WHEN** the collected folder is copied to a machine with networking disabled and the recipe's install steps are run
- **THEN** the install succeeds, `doctor` shows the converter as cached, and ingesting a corpus hoot succeeds without network

### Requirement: Privacy and network note
The docs SHALL have a privacy page that:
- states Flashpoint sends no telemetry;
- lists every outbound connection Flashpoint makes, and when it makes it (documentation links open the docs site only when clicked);
- says what a log and a lake contain (such as device serial numbers, match data and the team number);
- explains how to share logs privately for a bug report;
- explains that sharing the app on the pit network has no password.

#### Scenario: Nothing leaves the laptop
- **WHEN** Flashpoint ingests, derives, reports and serves a corpus match with the converter already cached and all outbound network blocked except loopback
- **THEN** every step succeeds and no outbound connection is attempted

#### Scenario: Every connection is listed
- **WHEN** the connections the page lists are compared with the features that open one (converter download, robot pull, backup, Live CAN)
- **THEN** each of those features is listed

### Requirement: Feature maturity labels
When the release labels a feature with a maturity label (such as robot pull "beta", Live CAN "experimental", or Anomalies "beta"), the docs SHALL show the same label at the top of every page about that feature, and in the support matrix. The README and the Why page SHALL NOT mention a labelled feature without its label.

#### Scenario: Anomalies labelled beta
- **WHEN** the Anomalies view shows a "beta" label
- **THEN** the Anomalies view reference, the triage how-to, the support matrix and the Why page all show "beta" wherever they mention Anomalies

### Requirement: Repository listing points to the docs
The GitHub repository's description SHALL be the README's one-line pitch. Its topics SHALL include `frc`, `wpilib`, `ctre`, `phoenix6`, `telemetry` and `datalog`, and its homepage SHALL be the docs site. The package metadata's Documentation URL SHALL be the docs site.

#### Scenario: Repository metadata
- **WHEN** the repository's metadata is read
- **THEN** the description matches the README pitch, the topics include the six above, and the homepage is `https://igniterobotics.github.io/flashpoint/`

#### Scenario: Package metadata
- **WHEN** the built wheel's metadata is read
- **THEN** its Documentation URL is the docs site
