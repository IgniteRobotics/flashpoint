# theme.toml

`theme.toml` sets the team name and number in the app header, and the app's colours. With no file, Flashpoint uses the [bundled default](https://github.com/IgniteRobotics/flashpoint/blob/main/src/flashpoint/defaults/theme.toml): amber, and the header reads "telemetry". [Set the team name and colours](../../how-to/team-name-and-colours.md) does this through the Setup view.

```toml
team_name = "Robo Rams"    # optional, at most 40 characters
# team_number = 1234       # optional, display only; defaults to `team` in acquire.toml
palette = "custom"         # amber, green, blue, red, white, or "custom"

[custom]                   # only with palette = "custom"
primary = "#00A3E0"        # #RRGGBB
# background = "#0A0806"   # optional
# warning = "#FF5A1F"      # optional; picked for you when left out
```

| Key | Default | What it does |
|---|---|---|
| `team_name` | none | The name in the header, 1 to 40 characters |
| `team_number` | `team` from `acquire.toml` | The number shown next to it. Display only |
| `palette` | `amber` | A preset (`amber`, `green`, `blue`, `red` or `white`, all on a dark background) or `custom` |
| `custom` | none | The `[custom]` table. Required with `palette = "custom"`, refused with a preset |
| `primary` | (required) | In `[custom]`: the main colour, `#RRGGBB` |
| `background` | the amber background | In `[custom]`: the page background |
| `warning` | worked out from `primary` | In `[custom]`: the warning colour |

Flashpoint works out every other shade from those colours, and refuses a palette that's hard to read:

- every text shade needs at least 4.5:1 contrast on every background shade;
- boundary lines need at least 3:1 on the background;
- the warning colour needs 4.5:1 too, and must stand apart from the primary: at least 35° apart in hue, or 25 % apart in lightness.

## Common errors

A broken `theme.toml` doesn't stop the app: it starts with the default amber and no name, and `flashpoint doctor` shows a `PROBLEM` on `theme.toml` with one of these:

- A misspelt key:
  ```text
  theme.toml: pallete: Extra inputs are not permitted
  ```
- An unknown palette:
  ```text
  theme.toml: palette 'purple' is not one of ['amber', 'green', 'blue', 'red', 'white', 'custom']
  ```
- `custom` without a `[custom]` table:
  ```text
  theme.toml: palette = "custom" needs a [custom] table with primary
  ```
- A colour that isn't `#RRGGBB`:
  ```text
  theme.toml: custom.primary: expected a #RRGGBB colour such as #00A3E0, not 'blue'
  ```
- A name over 40 characters:
  ```text
  theme.toml: team_name: String should have at most 40 characters
  ```
- A palette that fails the contrast rules. The message lists each failing pair and ends with a suggestion, for example:
  ```text
  theme.toml: palette refused: text contrast: accent #101010 on surface-raised #15100A is 1.01:1, needs at least 4.5:1; ... nearest primary that passes: #808080
  ```
