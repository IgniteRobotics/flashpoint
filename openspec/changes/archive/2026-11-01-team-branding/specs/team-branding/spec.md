## Purpose

Let a team show its own name and colours in Flashpoint, set up with the rest of its configuration, while every view stays readable and warnings stay recognisable.

## ADDED Requirements

### Requirement: Team display name
A theme configuration SHALL hold an optional team display name and an optional team number for display. The app header and page title SHALL show them (such as "Ignite Robotics · 6829") in every view, as text. A name SHALL be at most 40 characters, and longer names SHALL be refused when saved. With neither set, the header SHALL read "telemetry" and name no team. When no display number is set, the team number from the acquire configuration SHALL be shown.

#### Scenario: Name and number
- **WHEN** the theme names "Ignite Robotics" and the acquire configuration names team 6829
- **THEN** every view's header shows "Ignite Robotics · 6829", and the page title contains it

#### Scenario: No name
- **WHEN** no theme configuration exists and no team is set
- **THEN** the header reads "telemetry" and names no team

#### Scenario: Markup in the name
- **WHEN** the display name is `<b>Team</b>`
- **THEN** the header shows the literal text `<b>Team</b>`

### Requirement: Palette presets and custom palettes
The theme configuration SHALL select either a named preset or a custom palette. Flashpoint SHALL ship at least these presets: `amber` (the default and Ignite's current colours), `green`, `blue`, `red` and `white`, each on a dark background. A custom palette SHALL give a primary colour, and MAY give a background colour and a warning colour, each as a six-digit hex colour. From these Flashpoint SHALL derive every colour the app uses: the text shades (primary, emphasis, idle, secondary, tertiary, long-form text), the surfaces, the lines, the glows and the grid. The `amber` preset SHALL reproduce today's colours exactly.

#### Scenario: Default palette
- **WHEN** no theme configuration exists
- **THEN** the app's colours are identical to today's amber design tokens

#### Scenario: Custom primary
- **WHEN** the theme gives primary `#00A3E0` and nothing else
- **THEN** the header, active navigation, chart lines and selected rows use shades of `#00A3E0` on the default background

#### Scenario: Bad colour
- **WHEN** the theme gives primary `blue`
- **THEN** loading fails with a message naming the key and the expected `#RRGGBB` form

### Requirement: Palette validation
A palette SHALL be accepted only when:
- every derived text shade has a contrast ratio of at least 4.5:1 against every surface;
- every line meant to be seen as a boundary has at least 3:1 against the background;
- the warning colour has at least 4.5:1 against every surface, and is distinct from the primary colour (a hue difference of at least 35°, or a lightness difference of at least 25%).

A palette that fails SHALL be refused with a message naming each failing rule, the colours involved and their measured ratio, and, where one exists, the nearest primary or warning colour that would pass. When the warning colour isn't given, Flashpoint SHALL pick the first of its built-in warning colours that passes, and SHALL refuse the palette when none does.

#### Scenario: Too dark on dark
- **WHEN** the theme gives primary `#202020` on the default background
- **THEN** the palette is refused, naming the primary text shade, its ratio below 4.5:1, and a lighter primary that passes

#### Scenario: Warning too close to primary
- **WHEN** the theme gives primary `#FF4000` and no warning colour
- **THEN** the palette is accepted, with a warning colour from the built-in list that differs from `#FF4000` by at least 35° in hue

#### Scenario: Every preset passes
- **WHEN** each shipped preset is validated
- **THEN** each passes every rule

### Requirement: Theme applied everywhere
In served mode, the palette and name SHALL apply to every view, including charts and the Setup view, from the first page load and without a flash of the default colours. A static export SHALL carry the palette and name in force when it was written, and show them when opened from disk with no network. A theme file that fails to load SHALL NOT stop the app: the app SHALL use the default palette and no name, and the setup checks SHALL report the theme problem.

#### Scenario: Charts follow the palette
- **WHEN** the theme gives primary `#00A3E0` and Replay draws a match
- **THEN** the chart's primary series and cursor use the theme's shades, not amber

#### Scenario: Static export
- **WHEN** an export is written with the `green` preset and team name "Ignite Robotics", then opened from disk with networking disabled
- **THEN** it shows the green palette and "Ignite Robotics"

#### Scenario: Broken theme file
- **WHEN** `theme.toml` has an unknown key
- **THEN** the app starts with the default palette and no name, and the setup checks report a problem naming `theme.toml` and the key

### Requirement: Theme set during setup
`flashpoint init` SHALL accept a team display name and a preset, and SHALL ask for them when run in a terminal without them, offering the presets by name. The Setup view SHALL show an Appearance section with the current name and palette, a choice of preset or custom colours, and a live preview of the header, a chart and a warning before saving. Saving SHALL follow the configuration editor's guarded, validated save, and the whole app SHALL use the new theme on its next page load.

#### Scenario: Init with a preset
- **WHEN** `flashpoint init --team 254 --team-name "The Cheesy Poofs" --palette blue` runs on an empty configuration
- **THEN** `theme.toml` names "The Cheesy Poofs" and preset `blue`, and `serve` shows them

#### Scenario: Preview before saving
- **WHEN** a user picks custom primary `#00A3E0` in the Appearance section and has not saved
- **THEN** the preview shows the new colours, the rest of the app keeps the saved palette, and `theme.toml` is unchanged

#### Scenario: Refused in the view
- **WHEN** a user enters primary `#202020` and saves
- **THEN** the save is refused with the validation message and the suggested colour, and `theme.toml` is unchanged
