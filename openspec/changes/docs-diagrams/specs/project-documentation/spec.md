## ADDED Requirements

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
