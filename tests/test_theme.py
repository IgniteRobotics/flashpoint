"""Team theme model and colour maths (team-branding, design D-b, D-c, D-e)."""

import re
import time
from pathlib import Path

import pytest

from flashpoint import config, theme


@pytest.mark.parametrize(
    ("fg", "bg", "ratio"),
    [
        ("#000000", "#FFFFFF", 21.0),
        ("#FFFFFF", "#FFFFFF", 1.0),
        ("#777777", "#FFFFFF", 4.48),  # the classic just-fails-AA grey
        ("#FFB000", "#0A0806", 10.9),  # tokens.css: --accent on --bg
        ("#A27A36", "#0A0806", 5.1),  # tokens.css: --accent-muted on --bg
    ],
)
def test_contrast_ratio_matches_wcag(fg: str, bg: str, ratio: float) -> None:
    assert theme.contrast(fg, bg) == pytest.approx(ratio, abs=0.01 if ratio < 5 else 0.06)
    assert theme.contrast(bg, fg) == theme.contrast(fg, bg)  # symmetric


@pytest.mark.parametrize(
    ("hex_colour", "oklch"),
    [
        ("#FFFFFF", (1.0, 0.0, None)),
        ("#000000", (0.0, 0.0, None)),
        ("#FF0000", (0.62796, 0.25768, 29.2339)),
        ("#00FF00", (0.86644, 0.29483, 142.4953)),
        ("#0000FF", (0.45201, 0.31321, 264.052)),
    ],
)
def test_srgb_to_oklch_matches_reference(
    hex_colour: str, oklch: tuple[float, float, float | None]
) -> None:
    lightness, chroma, hue = theme.to_oklch(hex_colour)
    assert lightness == pytest.approx(oklch[0], abs=1e-4)
    assert chroma == pytest.approx(oklch[1], abs=1e-4)
    if oklch[2] is not None:
        assert hue == pytest.approx(oklch[2], abs=1e-2)


@pytest.mark.parametrize(
    "hex_colour", ["#FFB000", "#0A0806", "#00A3E0", "#7A5715", "#FF5A1F", "#123456", "#808080"]
)
def test_oklch_round_trips_to_the_same_hex(hex_colour: str) -> None:
    assert theme.from_oklch(*theme.to_oklch(hex_colour)) == hex_colour


def test_out_of_gamut_oklch_is_brought_into_srgb_keeping_hue() -> None:
    out = theme.from_oklch(0.9, 0.4, 264.0)  # far too saturated a light blue for sRGB
    assert len(out) == 7 and out.startswith("#")
    _, _, hue = theme.to_oklch(out)
    assert hue == pytest.approx(264.0, abs=3.0)


# ---- theme.toml (2.2) ---------------------------------------------------------------------


def _write(root: Path, text: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / theme.THEME_FILE).write_text(text, encoding="utf-8")
    return root


def test_no_theme_file_uses_the_bundled_default(tmp_path: Path) -> None:
    """Default palette: amber, no name."""
    loaded = theme.load_theme_config(tmp_path)
    assert loaded == theme.ThemeConfig()
    assert loaded.palette == "amber" and loaded.team_name is None and loaded.team_number is None
    assert loaded.custom is None
    assert (config.DEFAULTS_ROOT / theme.THEME_FILE).is_file()
    assert theme.load_theme_config(config.DEFAULTS_ROOT) == loaded


def test_preset_with_name_and_number(tmp_path: Path) -> None:
    root = _write(tmp_path, 'team_name = "Ignite Robotics"\nteam_number = 6829\npalette = "blue"\n')
    loaded = theme.load_theme_config(root)
    assert loaded.team_name == "Ignite Robotics"
    assert (loaded.team_number, loaded.palette) == (6829, "blue")


def test_custom_palette_with_optional_keys(tmp_path: Path) -> None:
    root = _write(tmp_path, 'palette = "custom"\n[custom]\nprimary = "#00a3e0"\n')
    loaded = theme.load_theme_config(root)
    assert loaded.custom == theme.CustomPalette(primary="#00A3E0")  # stored upper case
    assert loaded.custom.background is None and loaded.custom.warning is None
    full = _write(
        tmp_path / "full",
        'palette = "custom"\n[custom]\nprimary = "#00A3E0"\nbackground = "#06080A"\n'
        'warning = "#FF5A1F"\n',
    )
    assert theme.load_theme_config(full).custom == theme.CustomPalette(
        primary="#00A3E0", background="#06080A", warning="#FF5A1F"
    )


def test_name_longer_than_40_characters_is_refused(tmp_path: Path) -> None:
    assert theme.ThemeConfig(team_name="x" * 40).team_name == "x" * 40
    root = _write(tmp_path, f'team_name = "{"x" * 41}"\n')
    with pytest.raises(theme.ThemeError, match=r"(?s)theme\.toml.*team_name.*40"):
        theme.load_theme_config(root)


@pytest.mark.parametrize("value", ["blue", "#00A3E", "#00A3E0FF", "00A3E0", "#GGGGGG", "#0AF"])
def test_bad_colour_names_the_key_and_the_expected_form(tmp_path: Path, value: str) -> None:
    """Bad colour: loading fails with a message naming the key and #RRGGBB."""
    root = _write(tmp_path, f'palette = "custom"\n[custom]\nprimary = "{value}"\n')
    with pytest.raises(theme.ThemeError) as caught:
        theme.load_theme_config(root)
    message = str(caught.value)
    assert "theme.toml" in message and "primary" in message and "#RRGGBB" in message


@pytest.mark.parametrize(
    ("text", "needle"),
    [
        ('palette = "amber"\ncolour = "#FFB000"\n', "colour"),  # unknown key
        ('palette = "custom"\n[custom]\nprimary = "#00A3E0"\naccent = "#FFFFFF"\n', "accent"),
        ('palette = "purple"\n', "purple"),  # unknown preset
        ('palette = "custom"\n', "custom"),  # custom without [custom]
        ('palette = "blue"\n[custom]\nprimary = "#00A3E0"\n', "custom"),  # [custom] on a preset
        ("team_number = 0\n", "team_number"),
        ("palette = [\n", "theme.toml"),  # not TOML
    ],
)
def test_bad_theme_files_are_refused_naming_the_problem(
    tmp_path: Path, text: str, needle: str
) -> None:
    with pytest.raises(theme.ThemeError, match="theme.toml") as caught:
        theme.load_theme_config(_write(tmp_path, text))
    assert needle in str(caught.value)


# ---- Derivation (2.3) -----------------------------------------------------------------------

TODAY = {  # tokens.css colour values before team-branding (role names from design D-a)
    "bg": "#0A0806",
    "surface-1": "#0C0906",
    "surface-2": "#0D0A07",
    "surface-3": "#0F0B07",
    "surface-raised": "#15100A",
    "surface-drag": "#1A140B",
    "line-soft": "#241A0B",
    "line": "#3D2C10",
    "line-strong": "#7A5715",
    "accent": "#FFB000",
    "accent-hi": "#FFD27A",
    "accent-mid": "#E0A12A",
    "accent-dim": "#C98C1A",
    "accent-muted": "#A27A36",
    "prose": "#F2D49B",
    "accent-rgb": "255, 176, 0",
    "accent-hi-rgb": "255, 210, 122",
    "warn": "#FF5A1F",
    "warn-rgb": "255, 90, 31",
    "on-accent": "#0A0806",
    "bg-rgb": "10, 8, 6",
}
TEXT_SHADES = ("accent", "accent-hi", "accent-mid", "accent-dim", "accent-muted", "prose")


def _hue_gap(a: float, b: float) -> float:
    return abs((a - b + 180) % 360 - 180)


def test_amber_preset_is_todays_token_map_exactly() -> None:
    """Default palette: the amber preset is stored, not derived."""
    assert theme.preset_tokens("amber") == TODAY
    assert tuple(TODAY) == theme.TOKEN_NAMES


def test_amber_preset_matches_tokens_css() -> None:
    """The stored map and the shipped stylesheet agree, token by token."""
    css = (Path(theme.__file__).parent / "web" / "static" / "tokens.css").read_text()
    declared = dict(re.findall(r"^\s*--([a-z0-9-]+):\s*([^;]+);", css, re.MULTILINE))
    for name, value in theme.preset_tokens("amber").items():
        assert declared.get(name, "").strip() == value, name


def test_default_theme_resolves_to_amber(tmp_path: Path) -> None:
    """Default palette: no theme file gives today's tokens."""
    assert theme.load_theme(tmp_path).tokens == TODAY


def test_custom_primary_keeps_its_hue_across_text_shades() -> None:
    """Custom primary: shades of #00A3E0 on the default background."""
    tokens = theme.derive_tokens("#00A3E0")
    assert set(tokens) == set(TODAY)
    assert tokens["accent"] == "#00A3E0"
    assert tokens["accent-rgb"] == "0, 163, 224"
    assert tokens["bg"] == "#0A0806" and tokens["on-accent"] == "#0A0806"
    assert tokens["warn"] == "#FF5A1F"  # the first built-in warning colour
    _, _, hue = theme.to_oklch("#00A3E0")
    for name in (*TEXT_SHADES, "line-strong", "line"):
        assert _hue_gap(theme.to_oklch(tokens[name])[2], hue) < 3, name
    lightness = {name: theme.to_oklch(tokens[name])[0] for name in TEXT_SHADES}
    assert lightness["accent-hi"] > lightness["accent"] > lightness["accent-mid"]
    assert lightness["accent-mid"] > lightness["accent-dim"] > lightness["accent-muted"]


def test_derivation_reproduces_the_amber_scale_closely() -> None:
    """The offsets are measured from amber, so deriving from amber's inputs lands near it."""
    derived = theme.derive_tokens("#FFB000", "#0A0806", "#FF5A1F")
    for name, value in TODAY.items():
        if name.endswith("-rgb"):
            continue
        assert theme.to_oklch(derived[name])[0] == pytest.approx(
            theme.to_oklch(value)[0], abs=0.01
        ), name


@pytest.mark.parametrize("name", [*theme.PRESET_NAMES, "#00A3E0"])
def test_rgb_tokens_are_the_channels_of_their_colours(name: str) -> None:
    """The glows are built from these channels, so they follow the palette."""
    tokens = theme.preset_tokens(name) if name in theme.PRESET_NAMES else theme.derive_tokens(name)
    pairs = {
        "accent-rgb": "accent",
        "accent-hi-rgb": "accent-hi",
        "warn-rgb": "warn",
        "bg-rgb": "bg",
    }
    for rgb, colour in pairs.items():
        channels = (int(tokens[colour][i : i + 2], 16) for i in (1, 3, 5))
        assert tokens[rgb] == ", ".join(str(c) for c in channels), rgb


def test_surfaces_follow_the_background_hue() -> None:
    tokens = theme.derive_tokens("#00A3E0", background="#06080A")
    _, _, bg_hue = theme.to_oklch("#06080A")
    assert tokens["bg"] == "#06080A" and tokens["on-accent"] == "#06080A"
    for name in ("surface-1", "surface-2", "surface-3", "surface-raised", "surface-drag"):
        lightness, _, hue = theme.to_oklch(tokens[name])
        assert lightness > theme.to_oklch("#06080A")[0], name
        assert _hue_gap(hue, bg_hue) < 25, name  # near-black: 8-bit rounding moves the hue


# ---- Validation and suggestions (2.4) -------------------------------------------------------


def _custom(primary: str, **kw: str) -> theme.ThemeConfig:
    return theme.ThemeConfig(palette="custom", custom=theme.CustomPalette(primary=primary, **kw))


def test_too_dark_on_dark_is_refused_with_a_lighter_primary() -> None:
    """Too dark on dark: names the primary text shade, its ratio, and a lighter primary."""
    result = theme.resolve_palette(_custom("#202020"))
    assert not result.ok
    text = [f for f in result.failures if f.rule == theme.RULE_TEXT]
    accent = next(f for f in text if f.token == "accent")
    assert accent.colour == "#202020" and accent.ratio < 4.5
    assert "accent #202020" in accent.message and f"{accent.ratio:.2f}:1" in accent.message
    suggested = result.suggestion["primary"]
    assert theme.to_oklch(suggested)[0] > theme.to_oklch("#202020")[0]
    assert theme.resolve_palette(_custom(suggested)).ok
    assert suggested in result.message and "4.5:1" in result.message


def test_warning_too_close_to_primary_picks_a_distinct_builtin() -> None:
    """Warning too close to primary: #FF4000 with no warning gets a built-in 35 degrees away.
    (Its acceptance as a whole is in test_spec_example_primaries_are_accepted.)"""
    warn = theme.derive_tokens("#FF4000")["warn"]
    assert warn in theme.BUILTIN_WARNINGS and warn != "#FF5A1F"
    assert _hue_gap(theme.to_oklch(warn)[2], theme.to_oklch("#FF4000")[2]) >= theme.WARN_HUE_MIN
    result = theme.resolve_palette(_custom("#FF4000"))
    assert result.tokens["warn"] == warn
    assert theme.RULE_DISTINCT not in {f.rule for f in result.failures}
    assert theme.RULE_WARNING not in {f.rule for f in result.failures}


def test_a_given_warning_too_close_to_primary_is_refused_with_a_suggestion() -> None:
    # a grey primary has no hue, so only lightness can set the warning apart
    result = theme.resolve_palette(_custom("#E6E6E6", warning="#FF5A1F"))
    assert [f.rule for f in result.failures] == [theme.RULE_DISTINCT]
    assert "#FF5A1F" in result.message and "#E6E6E6" in result.message
    fixed = result.suggestion["warning"]
    assert "primary" not in result.suggestion
    assert theme.resolve_palette(_custom("#E6E6E6", warning=fixed)).ok
    # no lightness of #FFC020's hue is both far enough from #FFB000 and readable
    close = theme.resolve_palette(_custom("#FFB000", warning="#FFC020"))
    assert [f.rule for f in close.failures] == [theme.RULE_DISTINCT]
    assert close.suggestion == {}


def test_a_warning_without_contrast_is_refused() -> None:
    result = theme.resolve_palette(_custom("#33FF66", warning="#802000"))
    assert [f.rule for f in result.failures] == [theme.RULE_WARNING]
    assert "#802000" in result.message


def test_no_builtin_warning_passing_refuses_the_palette() -> None:
    result = theme.resolve_palette(_custom("#FFB000", background="#F0F0F0"))
    assert not result.ok
    assert "built-in warning" in result.message


def test_boundary_lines_need_three_to_one_on_the_background() -> None:
    assert theme.contrast(theme.AMBER_TOKENS["line-strong"], "#0A0806") >= 3.0
    weak = theme.AMBER_TOKENS | {"line-strong": "#3D2C10"}
    (failure,) = [f for f in theme.check_tokens(weak) if f.rule != theme.RULE_DISTINCT]
    assert failure.rule == theme.RULE_BOUNDARY and failure.token == "line-strong"
    assert failure.ratio < 3.0 and "3:1" in failure.message
    # the other lines are not boundaries and stay below 3:1 by design
    assert theme.contrast(theme.AMBER_TOKENS["line"], "#0A0806") < 3.0


@pytest.mark.parametrize(
    "name",
    theme.PRESET_NAMES,
)
def test_every_preset_passes(name: str) -> None:
    """Every preset passes: each shipped preset passes every rule."""
    result = theme.resolve_palette(theme.ThemeConfig(palette=name))
    assert result.ok, result.message
    assert theme.check_tokens(theme.preset_tokens(name)) == []


def test_presets_other_than_amber_are_stored_as_their_input_colours() -> None:
    assert set(theme.PRESETS) == set(theme.PRESET_NAMES) - {"amber"}
    for name, (primary, background, warning) in theme.PRESETS.items():
        assert theme.preset_tokens(name) == theme.derive_tokens(primary, background, warning)
        assert theme.contrast(background, "#000000") < 1.2, name  # each on a dark background


@pytest.mark.parametrize("primary", ["#00A3E0", "#FF4000"])
def test_spec_example_primaries_are_accepted(primary: str) -> None:
    """Custom primary, Warning too close to primary: the spec's examples are accepted."""
    result = theme.resolve_palette(_custom(primary))
    assert result.ok, result.message
    assert result.tokens["accent"] == primary


@pytest.mark.parametrize("primary", ["#00A3E0", "#FF4000", "#00C2FF", "#9B7BFF", "#3CB371"])
def test_shades_below_the_contrast_floor_are_compressed_in_order(primary: str) -> None:
    """Where the fixed offsets would put a darker shade below 4.5:1, the darker shades are
    lifted towards the primary, keeping their order and hue, until each is readable."""
    tokens = theme.derive_tokens(primary)
    _, _, hue = theme.to_oklch(primary)
    for name in TEXT_SHADES:
        assert theme._worst_surface(tokens, tokens[name])[1] >= 4.5, name
        assert _hue_gap(theme.to_oklch(tokens[name])[2], hue) < 3, name
    lightness = {name: theme.to_oklch(tokens[name])[0] for name in TEXT_SHADES}
    assert lightness["accent-hi"] > lightness["accent"] > lightness["accent-mid"]
    assert lightness["accent-mid"] > lightness["accent-dim"] > lightness["accent-muted"]


def test_shades_that_already_pass_keep_the_fixed_offsets() -> None:
    """The floor only moves shades that would be unreadable: every preset derives as before."""
    p_l, p_c, p_h = theme.to_oklch("#33FF66")
    tokens = theme.derive_tokens("#33FF66", "#060A07", "#FF5A1F")
    for name, (dl, kc) in theme._TEXT_STEPS.items():
        assert tokens[name] == theme.from_oklch(p_l + dl, p_c * kc, p_h), name


def test_a_primary_that_is_itself_unreadable_is_still_refused() -> None:
    result = theme.resolve_palette(_custom("#0000FF"))
    assert not result.ok
    assert "accent #0000FF" in result.message


@pytest.mark.parametrize("background", ["#000000", "#050505", "#020406"])
@pytest.mark.parametrize("primary", ["#FFB000", "#FFFFFF", "#33FF66", "#00A3E0"])
def test_pure_black_backgrounds_are_accepted(primary: str, background: str) -> None:
    """The boundary line is lifted to 3:1 on the background when its offset falls short."""
    result = theme.resolve_palette(_custom(primary, background=background))
    assert result.ok, result.message
    tokens = result.tokens
    assert theme.contrast(tokens["line-strong"], background) >= 3.0
    assert theme.to_oklch(tokens["line-strong"])[0] > theme.to_oklch(tokens["line"])[0]


def test_invalid_palette_fails_loading_naming_the_file(tmp_path: Path) -> None:
    root = _write(tmp_path, 'palette = "custom"\n[custom]\nprimary = "#202020"\n')
    with pytest.raises(theme.ThemeError, match=r"theme\.toml.*accent #202020"):
        theme.load_theme(root)


def test_loaded_theme_carries_name_and_tokens(tmp_path: Path) -> None:
    root = _write(tmp_path, 'team_name = "Ignite Robotics"\npalette = "green"\n')
    loaded = theme.load_theme(root)
    assert loaded.config.team_name == "Ignite Robotics"
    assert loaded.tokens == theme.preset_tokens("green")


def test_derivation_and_validation_stay_under_20_ms() -> None:
    """Budget: derivation plus validation < 20 ms (uncached), refusals with suggestions too."""
    primaries = ["#00C2FF", "#33FF66", "#FF4000", "#E6E6E6", "#9B7BFF", "#202020", "#0000FF"]
    worst = 0.0
    for primary in primaries:
        theme.clear_palette_cache()
        start = time.perf_counter()
        theme.resolve_palette(_custom(primary))
        worst = max(worst, time.perf_counter() - start)
    assert worst < 0.020, f"{worst * 1000:.1f} ms"
