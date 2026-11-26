## MODIFIED Requirements

### Requirement: Served and static modes
The app SHALL run in two modes:
- **served:** one command serves every view from a local lake over HTTP, bound to the local machine by default, with an option to share it on the pit network;
- **static:** an export command writes the Replay view and its data to a folder that opens by double-clicking its index page, with no server and no network.

Views that need the lake's query service (such as History) SHALL be absent from the navigation in static mode.

A static export SHALL NOT contain the lake's folder path or any other absolute path from the machine that made it, in its data files or on any page it shows. Served mode SHALL keep showing the lake's folder path. Exporting again into the same folder SHALL leave no lake path from an earlier export behind.

#### Scenario: Served
- **WHEN** the serve command is started on a corpus lake
- **THEN** Replay and History are reachable at the printed local address, and the command stops cleanly on interrupt

#### Scenario: Opened as a file
- **WHEN** a static export is opened directly from disk with networking disabled
- **THEN** the match list loads, a match's tracks render, and History is not in the navigation

#### Scenario: Export holds no lake path
- **WHEN** a static export of a synthetic lake with two matches is written, with and without raw files
- **THEN** no file in the export contains the lake's folder path, the export's match index has no lake field, and the match list still loads from disk

#### Scenario: Served mode keeps the lake path
- **WHEN** the same lake is served after the export
- **THEN** the header shows the lake's folder path, and the lake's own match index still records it

#### Scenario: Served files stay inside the app
- **WHEN** the server receives a request for a path outside the app, data, or raw-download locations (such as `/../ledger.sqlite`)
- **THEN** it answers not-found, and no file outside those locations is returned
