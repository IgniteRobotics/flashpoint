# Set the team name and colours

The app header can show your team's name and number, and the app can use your team's colours. Both live in `theme.toml` in your config folder. You can set them when you create the configuration, or later in the Setup view.

## Before you start

- Flashpoint is installed.
- For the Setup view: `flashpoint serve` runs on this laptop, started the usual way, and the app is open on this laptop. Shared with `--host 0.0.0.0`, Setup is read-only.
- Your colours as `#RRGGBB` codes, if you want your own instead of a preset, such as `#00A3E0`.

## On a new configuration: `flashpoint init`

`init` writes `theme.toml` along with the rest of the config folder. Give it the name and a preset:

```sh
flashpoint init --team <number> --team-name "<team name>" --palette blue
```

The presets are `amber` (the default), `green`, `blue`, `red` and `white`, all on a dark background. Leave out `--team-name` or `--palette` and `init` asks for them in a terminal; leave the name blank for none.

`init` never overwrites a file. If you already have a configuration, it prints `a configuration already exists in <folder>; nothing changed:` and changes nothing. Use the Setup view instead.

## On an existing configuration: the Setup view

1. Start the app with `flashpoint serve` and open **SETUP**, then **CONFIGURATION**.
2. Under **APPEARANCE**, in **TEAM NAME AND COLOURS**, fill in:
    - **TEAM NAME (HEADER, AT MOST 40 CHARACTERS)**;
    - **TEAM NUMBER (EMPTY: FROM ACQUIRE.TOML)**, only if the header should show a different number from your team number;
    - **COLOURS**: a preset, or "custom colours" for your own. Custom colours adds **PRIMARY**, **BACKGROUND (EMPTY: DEFAULT)** and **WARNING (EMPTY: AUTOMATIC)**, each as `#RRGGBB` with a colour picker beside it.
3. Look at the preview box under the fields. It redraws in the chosen colours, and says whether they're readable.
4. If it says "Not readable, so it can't be saved:", press the **USE PRIMARY** or **USE WARNING** button it offers. That puts the nearest colour that passes in the field.
5. Press the section's **SAVE**.
6. Reload the page. The rest of the app keeps the old colours until you do.

Flashpoint refuses colours that are hard to read: every text shade needs at least 4.5:1 contrast on every background, and the warning colour must stand apart from the primary. The [`theme.toml` reference](../reference/config/theme.md) has the exact rules and every key, if you'd rather edit the file.

A Replay export on a USB stick keeps the name and colours it was written with. Run `flashpoint report --static` again to update it ([Take Replay on a USB stick](replay-on-a-usb-stick.md)).

## Check it worked

- After **SAVE**, the section says "Saved theme.toml. Next: every view, on its next page load (reload the page)."
- The preview says "✓ Readable: every text shade has at least 4.5:1 contrast on every surface, and the warning colour stands apart from the primary."
- After a reload, the header reads **FLASHPOINT** telemetry // and your name and number, such as "Robo Rams · 1234", and the app uses your colours. With no name set, it shows only your team number.

If the app is back to amber with no name, `theme.toml` doesn't load: `flashpoint doctor` shows a `PROBLEM` on `theme.toml` with the reason. See [A settings file doesn't load](../troubleshooting.md#a-settings-file-doesnt-load) and the [common errors](../reference/config/theme.md#common-errors).
