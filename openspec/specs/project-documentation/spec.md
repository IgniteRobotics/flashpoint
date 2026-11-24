# project-documentation Specification

## Purpose

Documentation that tells a student, mentor, or developer what Flashpoint is for, how it works, how to build and run it, and how to set up automatic log import, kept in step with the code by automated checks.

## Requirements

### Requirement: README is an overview and entry point
The repository README SHALL say what Flashpoint does and who it's for in its opening section. It SHALL include a quickstart of five commands or fewer that goes from a fresh checkout to a served Replay view. It SHALL link to the user guide, the developer guide, the automatic import guide, and the CLI reference. The README SHALL NOT be organized by roadmap phase.

#### Scenario: New reader finds the purpose and the guides
- **WHEN** a reader opens the README
- **THEN** the first section states what Flashpoint ingests, what it tracks, and that it runs offline on a pit laptop, and the README links to all four guides

#### Scenario: No phase-organized sections
- **WHEN** the README's headings are listed
- **THEN** none of them names a roadmap phase (P0 to P6)

### Requirement: User guide
The documentation SHALL include a user guide for people who run Flashpoint but don't change its code. It SHALL cover:
- what Flashpoint is for, and what it isn't (it links to AdvantageScope for single-log deep dives rather than replacing it);
- installing on macOS, Linux, and Windows;
- ingesting logs by hand;
- serving the app and reading the Replay, History, and Anomalies views, including the shared filters and anomaly triage;
- building a static Replay export for a USB stick;
- describing a robot in its config file;
- where the lake lives and how to point Flashpoint at another one;
- reading the output of `flashpoint doctor`.

#### Scenario: First run from the user guide
- **WHEN** a reader follows the user guide's install and first-ingest steps on a supported OS, using a log from the golden corpus
- **THEN** each command succeeds as written, and the served app lists that log's match in Replay

#### Scenario: Offline preparation is called out
- **WHEN** a reader looks for what to do before taking a laptop to an event
- **THEN** the user guide says the hoot converter is downloaded on first use and must be fetched while online, and gives the command that does it

### Requirement: Developer guide
The documentation SHALL include a developer guide for people who change the code. It SHALL explain how Flashpoint works:
- the ingest pipeline from raw file to the bronze, silver, and gold layers;
- what each lake layer holds;
- match identity and session grouping;
- device identity (slot versus unit);
- clock alignment;
- detection.

It SHALL also cover building and running from source, the test markers and when each runs, fetching the golden corpus, what CI runs and which jobs the required check waits on, the coverage floors, the change workflow (specs, changes, branching), and where the decisions and research live. It SHALL carry the coding standards for Python and the app frontend, and the single command that runs every formatter and linter locally. It SHALL link to the target architecture and the ADRs rather than restating them in full.

#### Scenario: Build and test from the developer guide
- **WHEN** a reader with a supported Python version follows the developer guide's build and unit test steps on a fresh clone
- **THEN** the steps install the project and the unit test run passes

#### Scenario: Pipeline explained end to end
- **WHEN** a reader looks up what happens to a `.hoot` file after ingest
- **THEN** the developer guide traces it from the raw copy, through conversion and the bronze samples, to silver mapping and gold match features, and names the command that triggers each step

#### Scenario: Checks run locally before a push
- **WHEN** a reader follows the developer guide's local check step on a fresh clone of the default branch
- **THEN** one command runs every formatter and linter CI runs, and it passes

### Requirement: Automatic import guide
The documentation SHALL include a guide to automatic log import as it exists. It SHALL cover:
- what the watch pulls (roboRIO internal and USB log roots, and removable volumes on the pit machine) and that it never modifies or deletes anything on the robot or the stick;
- every key in the acquisition config file, with its default;
- a dry run before installing a service;
- installing the service on Linux and Windows;
- running it on macOS, where no service definition exists;
- optional backup and the restore drill;
- checking status;
- troubleshooting for: robot not reachable, USB volume not seen, file skipped as still being written, low robot disk space, and a stale lock.

It SHALL state plainly that importing from cloud folders is not supported.

#### Scenario: Linux service from the guide
- **WHEN** a reader follows the Linux steps on a machine with a user service manager
- **THEN** the unit file the guide names exists in the repository, and the commands enable it and show its log

#### Scenario: macOS limitation is explicit
- **WHEN** a macOS reader looks for how to run import in the background
- **THEN** the guide says there's no service definition for macOS and gives the foreground command to run instead

#### Scenario: Install command is correct
- **WHEN** a reader runs the install command the guide gives for a service install
- **THEN** it installs Flashpoint from this repository, not a package of the same name from a public index

### Requirement: CLI reference
The documentation SHALL include a CLI reference with one section for every `flashpoint` subcommand. Each section SHALL give its purpose, its options, and at least one example.

#### Scenario: Every subcommand documented
- **WHEN** the set of subcommands the CLI accepts is compared with the section headings in the CLI reference
- **THEN** each subcommand has a section

### Requirement: Documentation drift checks
The unit test suite SHALL check that every relative link in the README, the deploy README, and the guides points to a file that exists in the repository, and that every CLI subcommand has a section in the CLI reference. A failed check SHALL name the file, the line, and the broken link or the missing subcommand. The checks SHALL run on every pull request on Linux and Windows. They SHALL NOT need the network or the golden corpus.

#### Scenario: Broken link fails the suite
- **WHEN** a guide contains a relative link to a file that doesn't exist
- **THEN** the unit test run fails and names the guide, the line number, and the link target

#### Scenario: Undocumented subcommand fails the suite
- **WHEN** a new subcommand is added to the CLI without a section in the CLI reference
- **THEN** the unit test run fails and names the subcommand

#### Scenario: External links are not fetched
- **WHEN** a guide contains an absolute web link
- **THEN** the check skips it and makes no network request

#### Scenario: Malformed link syntax is reported, not crashed on
- **WHEN** a guide contains an unclosed link, such as `[text](path` with no closing parenthesis
- **THEN** the check reports that file and line as malformed instead of raising an unhandled error

### Requirement: README and docs home screenshots, one per row
The screenshots of the app's views in the README and on the docs home page SHALL appear one per row at the full width of the page, never side by side in a table or a grid. No docs page SHALL put an image in a table or a grid. Each screenshot SHALL come directly after a line that names its view and says what that view shows, and any label the view carries (such as "beta") SHALL be in that line. The README's images keep the rules of "README is an overview and entry point": absolute URLs under the docs site's `latest` media path, naming files from the media manifest.

#### Scenario: One screenshot per row
- **WHEN** the README is checked
- **THEN** no table contains an image, and each view screenshot sits alone in its paragraph, directly after the line describing it

#### Scenario: Description names the view
- **WHEN** the Replay, History and Anomalies screenshots are listed with the line before each
- **THEN** each line names its view, and the Anomalies line carries "beta"

#### Scenario: Images side by side are refused
- **WHEN** a README puts two screenshots in one table row
- **THEN** the unit test run fails and names the README line

#### Scenario: Docs home, one screenshot per row
- **WHEN** the docs home page's Replay, History and Anomalies screenshots are listed with the line before each
- **THEN** none is inside a grid or a table, each sits alone in its paragraph, and each line names its view, the Anomalies line with "beta"

#### Scenario: Images in a grid are refused
- **WHEN** a docs page puts an image inside a `grid` block or a table row
- **THEN** the unit test run fails and names the page and the line

### Requirement: Diagrams render as pictures
A diagram written as a `mermaid` code block on a docs page SHALL render as a picture, both on the published site and in the offline archive opened from disk with no network. Rendering SHALL need no host other than the docs site itself. A page with no diagram SHALL NOT load the diagram renderer. A docs build that can't provide the renderer it was pinned to SHALL fail rather than publish pages whose diagrams show as source.

#### Scenario: Pipeline diagram on How Flashpoint works
- **WHEN** the built "How Flashpoint works" page is opened in a browser with requests to every other host blocked
- **THEN** the pipeline diagram shows as a picture, its source text isn't shown, and the console has no errors

#### Scenario: Diagram in the offline archive
- **WHEN** the offline archive's "How Flashpoint works" page is opened from disk with networking disabled
- **THEN** the pipeline diagram shows as a picture

#### Scenario: Page without a diagram
- **WHEN** a built page with no diagram is opened
- **THEN** the diagram renderer isn't requested

#### Scenario: Renderer doesn't match its pin
- **WHEN** the fetched renderer's checksum doesn't match the pinned one
- **THEN** the docs build step exits non-zero, names the file and the checksum, and nothing is published

### Requirement: Animations show each step
Each animation in the media manifest SHALL show its flow at a pace a reader can follow:
- the first frame shows the starting view already loaded, never a loading screen;
- the starting view is held for at least 1.5 seconds before the first action;
- each element is marked on screen just before it's clicked;
- the result is held for at least 2 seconds at the end.

Each animation SHALL last at least 6 seconds and at most 20 seconds. The capture command SHALL fail, naming the entry, when an animation is shorter than that, or when its first frame shows the app's loading screen.

#### Scenario: History animation on the Why page
- **WHEN** `flow-history-motor` is captured from the sample lake
- **THEN** it opens on History with the chart loaded, marks the motor before selecting it, ends holding the selected motor's details, and lasts between 6 and 20 seconds

#### Scenario: Clip too short
- **WHEN** a recorded animation lasts less than 6 seconds
- **THEN** the capture command exits non-zero and names the entry and its length

#### Scenario: Loading screen in the first frame
- **WHEN** an animation's first frame shows the app's loading screen
- **THEN** the capture command exits non-zero and names the entry

### Requirement: Images open full-frame
On every docs page, a screenshot, GIF or animation SHALL open at the full size of the browser window when clicked or activated from the keyboard, keeping its aspect ratio, on a dark backdrop, with its alt text available as a caption. An opened animation SHALL keep playing. Clicking the opened image, clicking the backdrop, pressing Esc or the close button SHALL return to the page at the same scroll position. Zoom SHALL work in the offline archive and SHALL need no host other than the docs site. Images that are links, and the site's logo and icons, SHALL NOT open full-frame.

#### Scenario: Open and close a screenshot
- **WHEN** a reader clicks the Replay screenshot on the docs home page, then clicks the opened image
- **THEN** the screenshot first fills the window with its whole frame visible, then the page returns as it was, at the same scroll position

#### Scenario: Keyboard
- **WHEN** a reader moves focus to a screenshot, presses Enter, then presses Esc
- **THEN** the screenshot opens full-frame, closes again, and focus returns to it

#### Scenario: Animation opens and keeps playing
- **WHEN** a reader clicks the History animation on the Why page
- **THEN** it opens full-frame and keeps playing

#### Scenario: Offline zoom
- **WHEN** a screenshot is clicked in the offline archive opened from disk with networking disabled
- **THEN** it opens full-frame, and no request leaves the archive's folder

### Requirement: Diagrams fit the docs column
Every diagram on a docs page SHALL be drawn at 80 % of its natural size or more in the page's content column on a desktop browser (a 1280 px wide window), so its labels stay close to body-text size. A docs build whose pages hold a diagram drawn smaller SHALL fail, naming the page, which diagram on it, and the size it was drawn at. The developer guide SHALL give the rules that keep diagrams inside the limit: flows run top to bottom, at most about four boxes sit side by side, and a diagram that would exceed the limit is split. The rules SHALL apply to the diagrams in the design documents as well.

#### Scenario: Pipeline on How Flashpoint works
- **WHEN** the built "How Flashpoint works" page is opened in a 1280 px wide window
- **THEN** the pipeline diagram is drawn at 80 % of its natural size or more, with the pipeline flowing from the robot at the top to the app's views at the bottom

#### Scenario: A diagram too wide for the column
- **WHEN** a docs page holds a diagram whose natural width is more than 1.25 times the content column, such as a nine-box left-to-right chain
- **THEN** the docs test run fails and names the page, the diagram's position on the page, and the percentage it was drawn at

#### Scenario: The rules are written down
- **WHEN** a contributor reads the developer guide before drawing a diagram
- **THEN** it states the 80 % limit, the top-to-bottom and four-across rules, and splitting, with the pipeline as the example

### Requirement: Diagrams open full-frame
On every docs page, a diagram SHALL open full-frame when clicked or activated from the keyboard, on the same backdrop as an opened image. It SHALL be drawn sharp, whole inside the browser window and as large as the window allows: larger than on the page whenever the window has room for that. A diagram too tall for the window to show larger opens whole, as an overview. It SHALL close the same way an opened image does (click, Esc or the close button), returning to the page at the same scroll position with focus back on the diagram. Diagram zoom SHALL work in the offline archive and SHALL need no host other than the docs site.

#### Scenario: Open and close the pipeline
- **WHEN** a reader clicks the pipeline diagram on "How Flashpoint works", then presses Esc
- **THEN** the diagram first fills the window's height with all of it visible, then the page returns at the same scroll position with focus on the diagram

#### Scenario: A diagram that fits opens larger
- **WHEN** a reader opens the file-lifecycle diagram on "How Flashpoint works" in a 1280 × 800 window
- **THEN** all of it is visible, drawn larger than on the page

#### Scenario: Keyboard
- **WHEN** a reader moves focus to a diagram and presses Enter
- **THEN** the diagram opens full-frame

#### Scenario: Opened diagram in the other palette
- **WHEN** a reader opens a diagram in the dark palette
- **THEN** the opened diagram uses the dark palette's colours, as the diagram on the page does

#### Scenario: Offline diagram zoom
- **WHEN** a diagram is opened in the offline archive opened from disk with networking disabled
- **THEN** it opens full-frame, and no request leaves the archive's folder

### Requirement: One diagram notation
Diagrams on docs pages SHALL share one notation. Each box SHALL be one of four kinds, each with its own colour and shape, and the same kind SHALL look the same on every page:
- a step Flashpoint runs;
- data Flashpoint keeps;
- the robot or something on it;
- something outside Flashpoint or optional, drawn with a dashed outline.

In a diagram of states, a state that's an error (such as a quarantined file) SHALL be marked as one. The colours SHALL come from the docs theme, not from a separate brand palette, and SHALL follow the docs' light/dark toggle, the reader's explicit choice included. A diagram that uses the kinds SHALL have a legend next to it naming each kind it shows. Labels SHALL be drawn as text that every supported browser shows whole, with no line cut off at the bottom of its box. Each box SHALL be sized for the font its label is drawn in.

#### Scenario: Pipeline in both palettes
- **WHEN** "How Flashpoint works" is opened in the light palette, then switched to dark with the page's toggle
- **THEN** the pipeline's steps, data and robot boxes keep three different colours in each palette, the colours change with the toggle, and the legend below the diagram names those kinds

#### Scenario: File lifecycle
- **WHEN** a reader looks for what can happen to a log file on "How Flashpoint works"
- **THEN** a diagram shows its states (raw, converted, quarantined, failed and retried) in the same notation, with quarantined and failed marked as the error states, and the prose next to it agrees with it

#### Scenario: Labels fit their boxes
- **WHEN** each diagram on the built docs site is checked for its labels
- **THEN** every label is drawn as plain text inside its box, and no label's text runs past its box's edge
