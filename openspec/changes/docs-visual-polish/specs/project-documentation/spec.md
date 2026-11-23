## ADDED Requirements

### Requirement: README screenshots, one per row
The README's screenshots of the app's views SHALL appear one per row at the full width of the page, never side by side in a table. Each screenshot SHALL come directly after a line that names its view and says what that view shows, and any label the view carries (such as "beta") SHALL be in that line. The images keep the rules of "README is an overview and entry point": absolute URLs under the docs site's `latest` media path, naming files from the media manifest.

#### Scenario: One screenshot per row
- **WHEN** the README is checked
- **THEN** no table contains an image, and each view screenshot sits alone in its paragraph, directly after the line describing it

#### Scenario: Description names the view
- **WHEN** the Replay, History and Anomalies screenshots are listed with the line before each
- **THEN** each line names its view, and the Anomalies line carries "beta"

#### Scenario: Images side by side are refused
- **WHEN** a README puts two screenshots in one table row
- **THEN** the unit test run fails and names the README line

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
