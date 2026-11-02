"""Team theme: display name and colour palette (`theme.toml`), derived into design tokens.

The colour maths (WCAG 2 contrast, sRGB <-> OKLCH) is here so the server, the static export,
`init` and the Setup view share one implementation (team-branding design D-b, D-c).
"""

import math
import re
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationError, model_validator

from flashpoint.config import config_file

THEME_FILE = "theme.toml"
NAME_MAX = 40
CUSTOM = "custom"
DEFAULT_PALETTE = "amber"
PRESET_NAMES = (DEFAULT_PALETTE, "green", "blue", "red", "white")

# ---- Colour maths -------------------------------------------------------------------------

Oklch = tuple[float, float, float]  # lightness 0..1, chroma >= 0, hue in degrees 0..360


def _rgb(hex_colour: str) -> tuple[int, int, int]:
    value = int(hex_colour[1:7], 16)
    return value >> 16, (value >> 8) & 0xFF, value & 0xFF


def _hex(rgb: tuple[float, float, float]) -> str:
    return "#" + "".join(f"{round(min(1.0, max(0.0, c)) * 255):02X}" for c in rgb)


def _linear(channel: float) -> float:
    return channel / 12.92 if channel <= 0.04045 else math.pow((channel + 0.055) / 1.055, 2.4)


def _gamma(channel: float) -> float:
    """Linear light to the sRGB curve (out-of-gamut channels may be negative)."""
    size = abs(channel)
    encoded = 12.92 * size if size <= 0.0031308 else 1.055 * math.pow(size, 1 / 2.4) - 0.055
    return math.copysign(encoded, channel)


def luminance(hex_colour: str) -> float:
    """WCAG 2 relative luminance."""
    r, g, b = (_linear(c / 255) for c in _rgb(hex_colour))
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast(a: str, b: str) -> float:
    """WCAG 2 contrast ratio between two `#RRGGBB` colours, 1..21."""
    la, lb = luminance(a), luminance(b)
    return (max(la, lb) + 0.05) / (min(la, lb) + 0.05)


def _to_oklab(rgb: tuple[float, float, float]) -> tuple[float, float, float]:
    r, g, b = (_linear(c) for c in rgb)
    lms = (
        0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b,
        0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b,
        0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b,
    )
    l_, m_, s_ = (math.copysign(abs(v) ** (1 / 3), v) for v in lms)
    return (
        0.2104542553 * l_ + 0.7936177850 * m_ - 0.0040720468 * s_,
        1.9779984951 * l_ - 2.4285922050 * m_ + 0.4505937099 * s_,
        0.0259040371 * l_ + 0.7827717662 * m_ - 0.8086757660 * s_,
    )


def _from_oklab(lightness: float, a: float, b: float) -> tuple[float, float, float]:
    """Gamma-encoded sRGB channels 0..1, possibly out of gamut."""
    l_ = lightness + 0.3963377774 * a + 0.2158037573 * b
    m_ = lightness - 0.1055613458 * a - 0.0638541728 * b
    s_ = lightness - 0.0894841775 * a - 1.2914855480 * b
    l3, m3, s3 = l_**3, m_**3, s_**3
    r = 4.0767416621 * l3 - 3.3077115913 * m3 + 0.2309699292 * s3
    g = -1.2684380046 * l3 + 2.6097574011 * m3 - 0.3413193965 * s3
    b_ = -0.0041960863 * l3 - 0.7034186147 * m3 + 1.7076147010 * s3
    return _gamma(r), _gamma(g), _gamma(b_)


def to_oklch(hex_colour: str) -> Oklch:
    r, g, b_ = _rgb(hex_colour)
    lightness, a, b = _to_oklab((r / 255, g / 255, b_ / 255))
    hue = math.degrees(math.atan2(b, a)) % 360
    return lightness, math.hypot(a, b), hue


_GAMUT_EPS = 1e-4


def _in_gamut(rgb: tuple[float, float, float]) -> bool:
    return all(-_GAMUT_EPS <= c <= 1 + _GAMUT_EPS for c in rgb)


def _oklch_rgb(lightness: float, chroma: float, hue: float) -> tuple[float, float, float]:
    rad = math.radians(hue)
    return _from_oklab(lightness, chroma * math.cos(rad), chroma * math.sin(rad))


def from_oklch(lightness: float, chroma: float, hue: float) -> str:
    """`#RRGGBB` for an OKLCH colour. Out of gamut, chroma is reduced (lightness and hue kept)
    until it fits sRGB."""
    lightness = min(1.0, max(0.0, lightness))
    chroma = max(0.0, chroma)
    rgb = _oklch_rgb(lightness, chroma, hue)
    if not _in_gamut(rgb):
        low, high = 0.0, chroma
        for _ in range(20):
            mid = (low + high) / 2
            if _in_gamut(_oklch_rgb(lightness, mid, hue)):
                low = mid
            else:
                high = mid
        rgb = _oklch_rgb(lightness, low, hue)
    return _hex(rgb)


# ---- theme.toml -----------------------------------------------------------------------------

_HEX = re.compile(r"^#[0-9A-Fa-f]{6}$")


class ThemeError(ValueError):
    """A theme file or palette that can't be used; the message names the file, key or rule."""


def _hex_colour(value: str) -> str:
    if not _HEX.match(value):
        raise ValueError(f"expected a #RRGGBB colour such as #00A3E0, not {value!r}")
    return value.upper()


HexColour = Annotated[str, AfterValidator(_hex_colour)]


class CustomPalette(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    primary: HexColour
    background: HexColour | None = None
    warning: HexColour | None = None


class ThemeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    team_name: str | None = Field(default=None, min_length=1, max_length=NAME_MAX)
    team_number: int | None = Field(default=None, ge=1, le=99999)  # display only
    palette: str = DEFAULT_PALETTE
    custom: CustomPalette | None = None

    @model_validator(mode="after")
    def _palette(self) -> "ThemeConfig":
        if self.palette == CUSTOM:
            if self.custom is None:
                raise ValueError('palette = "custom" needs a [custom] table with primary')
        elif self.palette not in PRESET_NAMES:
            raise ValueError(f"palette {self.palette!r} is not one of {[*PRESET_NAMES, CUSTOM]}")
        elif self.custom is not None:
            raise ValueError(f'[custom] is only used with palette = "custom", not {self.palette!r}')
        return self


def parse_theme_config(data: dict[str, Any], name: str = THEME_FILE) -> ThemeConfig:
    try:
        return ThemeConfig.model_validate(data)
    except ValidationError as exc:
        raise ThemeError(f"{name}: {exc}") from exc


def load_theme_config(config_dir: Path) -> ThemeConfig:
    """The team's `theme.toml`, else the bundled default (amber, no name). Unknown keys, bad
    colours and names over 40 characters raise ThemeError naming the file and key."""
    path = config_file(config_dir, THEME_FILE)
    if not path.is_file():
        return ThemeConfig()
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ThemeError(f"{path.name}: {exc}") from exc
    return parse_theme_config(data, path.name)


# ---- Palettes and derivation (design D-b) ---------------------------------------------------

# Today's tokens.css colours, by role name (design D-a). The amber preset IS this map, so the
# default look can't drift through derivation rounding.
AMBER_TOKENS: dict[str, str] = {
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
    "accent-rgb": "255, 176, 0",  # the glows and grid in tokens.css are built from this
    "accent-hi-rgb": "255, 210, 122",  # *-rgb: channels for the glows built from that colour
    "warn": "#FF5A1F",
    "warn-rgb": "255, 90, 31",
    "on-accent": "#0A0806",
    "bg-rgb": "10, 8, 6",
}
TOKEN_NAMES = tuple(AMBER_TOKENS)
DEFAULT_BACKGROUND = AMBER_TOKENS["bg"]
SURFACES = ("bg", "surface-1", "surface-2", "surface-3", "surface-raised", "surface-drag")
TEXT_SHADES = ("accent", "accent-hi", "accent-mid", "accent-dim", "accent-muted", "prose")
LINES = ("line-soft", "line", "line-strong")
BOUNDARY_LINES = ("line-strong",)  # seen as a boundary: >= 3:1 on the background
TEXT_MIN = 4.5  # every text shade on every surface
BOUNDARY_MIN = 3.0  # boundary lines on the background
# Tried in order when a palette gives no warning colour (design D-c); the first is today's.
BUILTIN_WARNINGS = ("#FF5A1F", "#00C2FF", "#FF3DAE", "#FFE14D")


def _offsets(
    names: tuple[str, ...], light_from: str, chroma_from: str
) -> dict[str, tuple[float, float]]:
    """Per token: (lightness offset from `light_from`, chroma as a share of `chroma_from`'s),
    measured on the amber map."""
    base_l = to_oklch(AMBER_TOKENS[light_from])[0]
    base_c = to_oklch(AMBER_TOKENS[chroma_from])[1]
    out = {}
    for name in names:
        lightness, chroma, _ = to_oklch(AMBER_TOKENS[name])
        out[name] = (lightness - base_l, chroma / base_c)
    return out


# Text shades step from the primary; lines sit above the background in the primary's hue;
# surfaces step from the background in its own hue.
_TEXT_STEPS = _offsets(TEXT_SHADES[1:], "accent", "accent")
_LINE_STEPS = _offsets(LINES, "bg", "accent")
_SURFACE_STEPS = _offsets(SURFACES[1:], "bg", "bg")


def _rgb_token(hex_colour: str) -> str:
    return ", ".join(str(c) for c in _rgb(hex_colour))


_FLOOR_MARGIN = 0.002  # OKLCH lightness above the floor, so 8-bit rounding can't undercut it


def _lightness_floor(
    chroma: float, hue: float, against: list[str], minimum: float, start: float
) -> float | None:
    """The lowest OKLCH lightness at or above `start` (at this chroma and hue) whose colour has
    `minimum` contrast on each of `against`, or None when even white falls short."""

    def passes(lightness: float) -> bool:
        colour = from_oklch(lightness, chroma, hue)
        return all(contrast(colour, s) >= minimum for s in against)

    if passes(start):
        return start
    if not passes(1.0):
        return None
    low, high = start, 1.0
    for _ in range(16):
        mid = (low + high) / 2
        if passes(mid):
            high = mid
        else:
            low = mid
    return min(1.0, high + _FLOOR_MARGIN)


def _text_lightness(
    p_l: float, p_c: float, p_h: float, surfaces: list[str]
) -> dict[str, tuple[float, float]]:
    """Per text shade: (lightness, chroma). The shades step from the primary by the fixed
    offsets (D-b). Where that would put a darker shade below 4.5:1 on a surface, the darker
    shades' offsets are scaled down together, keeping their order, until each one reads; a
    primary that is itself unreadable is left to validation."""
    steps = {name: (p_l + dl, p_c * kc) for name, (dl, kc) in _TEXT_STEPS.items()}
    primary = from_oklch(p_l, p_c, p_h)
    if any(contrast(primary, s) < TEXT_MIN for s in surfaces):
        return steps
    scale = 1.0
    for dl, kc in _TEXT_STEPS.values():
        if dl >= 0:
            continue
        floor = _lightness_floor(p_c * kc, p_h, surfaces, TEXT_MIN, p_l + dl)
        if floor is None or floor >= p_l:
            return steps
        scale = min(scale, (p_l - floor) / -dl)
    return {
        name: (p_l + dl * scale if dl < 0 else p_l + dl, p_c * kc)
        for name, (dl, kc) in _TEXT_STEPS.items()
    }


def _derive(primary: str, background: str, warning: str) -> dict[str, str]:
    p_l, p_c, p_h = to_oklch(primary)
    b_l, b_c, b_h = to_oklch(background)
    tokens = {"bg": background, "accent": primary}
    for name, (dl, kc) in _SURFACE_STEPS.items():
        tokens[name] = from_oklch(b_l + dl, b_c * kc, b_h)
    for name, (dl, kc) in _LINE_STEPS.items():
        lightness = b_l + dl
        if name in BOUNDARY_LINES:  # lifted to 3:1 on the background where the offset falls short
            lightness = (
                _lightness_floor(p_c * kc, p_h, [background], BOUNDARY_MIN, lightness) or lightness
            )
        tokens[name] = from_oklch(lightness, p_c * kc, p_h)
    surfaces = [tokens[s] for s in SURFACES]
    for name, (lightness, chroma) in _text_lightness(p_l, p_c, p_h, surfaces).items():
        tokens[name] = from_oklch(lightness, chroma, p_h)
    tokens["warn"] = warning
    tokens["on-accent"] = background
    return _with_rgb(tokens)


_RGB_TOKENS = {
    "accent-rgb": "accent",
    "accent-hi-rgb": "accent-hi",
    "warn-rgb": "warn",
    "bg-rgb": "bg",
}


def _with_rgb(tokens: Mapping[str, str]) -> dict[str, str]:
    """The token map in TOKEN_NAMES order, with each `*-rgb` token set from its colour."""
    full = {**tokens, **{rgb: _rgb_token(tokens[colour]) for rgb, colour in _RGB_TOKENS.items()}}
    return {name: full[name] for name in TOKEN_NAMES}


def derive_tokens(
    primary: str, background: str = DEFAULT_BACKGROUND, warning: str | None = None
) -> dict[str, str]:
    """Every colour token from a primary, a background and a warning colour (`#RRGGBB`). With
    no warning, the first built-in warning colour that passes the warning rules is used (the
    first one if none passes, which validation then refuses)."""
    primary, background = primary.upper(), background.upper()
    if warning is not None:
        return _derive(primary, background, warning.upper())
    first = _derive(primary, background, BUILTIN_WARNINGS[0])
    for candidate in BUILTIN_WARNINGS:
        tokens = _with_rgb(first | {"warn": candidate})
        if not _warning_failures(tokens):
            return tokens
    return first


# ---- Validation and suggestions (design D-c) ------------------------------------------------

WARN_HUE_MIN = 35.0  # degrees of OKLCH hue
WARN_LIGHTNESS_MIN = 0.25  # OKLCH lightness, 0..1
ACHROMATIC = 0.02  # below this OKLCH chroma a colour has no meaningful hue
RULE_TEXT = "text contrast"
RULE_BOUNDARY = "boundary line"
RULE_WARNING = "warning contrast"
RULE_DISTINCT = "warning distinct from primary"


@dataclass(frozen=True)
class Failure:
    rule: str
    token: str
    colour: str
    against: str  # the surface token, or "accent" for the distinctness rule
    ratio: float  # measured contrast ratio (for distinctness: the hue gap in degrees)
    message: str


def _worst_surface(tokens: Mapping[str, str], colour: str) -> tuple[str, float]:
    return min(((s, contrast(colour, tokens[s])) for s in SURFACES), key=lambda pair: pair[1])


def _contrast_failure(
    tokens: Mapping[str, str], rule: str, token: str, minimum: float, surfaces: tuple[str, ...]
) -> Failure | None:
    colour = tokens[token]
    surface, ratio = min(
        ((s, contrast(colour, tokens[s])) for s in surfaces), key=lambda pair: pair[1]
    )
    if ratio >= minimum:
        return None
    return Failure(
        rule,
        token,
        colour,
        surface,
        ratio,
        f"{rule}: {token} {colour} on {surface} {tokens[surface]} is {ratio:.2f}:1,"
        f" needs at least {minimum:g}:1",
    )


def _hue_gap(a: float, b: float) -> float:
    return abs((a - b + 180) % 360 - 180)


def _warning_failures(tokens: Mapping[str, str]) -> list[Failure]:
    failures = []
    low = _contrast_failure(tokens, RULE_WARNING, "warn", TEXT_MIN, SURFACES)
    if low is not None:
        failures.append(low)
    warn, accent = tokens["warn"], tokens["accent"]
    w_l, w_c, w_h = to_oklch(warn)
    a_l, a_c, a_h = to_oklch(accent)
    hue_gap = _hue_gap(w_h, a_h) if min(w_c, a_c) >= ACHROMATIC else 0.0
    light_gap = abs(w_l - a_l)
    if hue_gap < WARN_HUE_MIN and light_gap < WARN_LIGHTNESS_MIN:
        failures.append(
            Failure(
                RULE_DISTINCT,
                "warn",
                warn,
                "accent",
                hue_gap,
                f"{RULE_DISTINCT}: warn {warn} and accent {accent} are {hue_gap:.0f}° apart in"
                f" hue and {light_gap:.0%} in lightness, needs {WARN_HUE_MIN:g}° or"
                f" {WARN_LIGHTNESS_MIN:.0%}",
            )
        )
    return failures


def _primary_failures(tokens: Mapping[str, str]) -> list[Failure]:
    found = [_contrast_failure(tokens, RULE_TEXT, t, TEXT_MIN, SURFACES) for t in TEXT_SHADES]
    found += [
        _contrast_failure(tokens, RULE_BOUNDARY, t, BOUNDARY_MIN, ("bg",)) for t in BOUNDARY_LINES
    ]
    return [f for f in found if f is not None]


def check_tokens(tokens: Mapping[str, str]) -> list[Failure]:
    """Every rule a token map breaks: text shades >= 4.5:1 on every surface, boundary lines
    >= 3:1 on the background, the warning >= 4.5:1 on every surface and distinct from the
    primary (35° of hue or 25% of lightness)."""
    return _primary_failures(tokens) + _warning_failures(tokens)


@dataclass(frozen=True)
class PaletteResult:
    tokens: dict[str, str]
    failures: tuple[Failure, ...] = ()
    suggestion: dict[str, str] = field(default_factory=dict)  # "primary" / "warning" -> hex
    note: str = ""  # e.g. that no built-in warning colour passes

    @property
    def ok(self) -> bool:
        return not self.failures

    @property
    def message(self) -> str:
        if self.ok:
            return ""
        parts = [f.message for f in self.failures]
        if self.note:
            parts.append(self.note)
        parts += [f"nearest {key} that passes: {value}" for key, value in self.suggestion.items()]
        return "palette refused: " + "; ".join(parts)


def _nearest(colour: str, passes: Callable[[str], bool]) -> str | None:
    """The passing colour nearest `colour` in OKLCH lightness, hue and chroma kept (D-c)."""
    l0, chroma, hue = to_oklch(colour)
    passing = [n / 20 for n in range(21) if passes(from_oklch(n / 20, chroma, hue))]
    if not passing:
        return None
    good = min(passing, key=lambda lightness: abs(lightness - l0))
    bad = l0
    for _ in range(10):  # bisect towards the original lightness
        mid = (good + bad) / 2
        if passes(from_oklch(mid, chroma, hue)):
            good = mid
        else:
            bad = mid
    return from_oklch(good, chroma, hue)


@lru_cache(maxsize=64)
def _resolve(primary: str, background: str, warning: str | None) -> PaletteResult:
    tokens = derive_tokens(primary, background, warning)
    failures = check_tokens(tokens)
    if not failures:
        return PaletteResult(tokens)
    note = ""
    if warning is None and _warning_failures(tokens):
        note = f"no built-in warning colour ({', '.join(BUILTIN_WARNINGS)}) passes"
    suggestion: dict[str, str] = {}
    fixed_primary = primary
    if _primary_failures(tokens) or (warning is None and note):

        def primary_passes(candidate: str) -> bool:
            derived = derive_tokens(candidate, background, warning)
            rules = check_tokens if warning is None else _primary_failures
            return not rules(derived)

        found = _nearest(primary, primary_passes)
        if found is not None:
            suggestion["primary"] = fixed_primary = found
    if warning is not None and _warning_failures(tokens):
        base = derive_tokens(fixed_primary, background, warning)
        found = _nearest(warning, lambda c: not _warning_failures(base | {"warn": c}))
        if found is not None:
            suggestion["warning"] = found
    return PaletteResult(tokens, tuple(failures), suggestion, note)


# Presets other than amber, stored as their input colours (primary, background, warning).
PRESETS: dict[str, tuple[str, str, str]] = {
    "green": ("#33FF66", "#060A07", "#FF5A1F"),
    "blue": ("#6CD0FF", "#06080A", "#FF5A1F"),
    "red": ("#FFADAD", "#0E0808", "#FFE14D"),
    "white": ("#F0F0F0", "#080808", "#FF5A1F"),
}


def preset_tokens(name: str) -> dict[str, str]:
    if name == DEFAULT_PALETTE:
        return dict(AMBER_TOKENS)
    return derive_tokens(*PRESETS[name])


def resolve_palette(theme_config: ThemeConfig) -> PaletteResult:
    """The token map for a theme's palette, with every rule it breaks and the nearest passing
    colours. The amber preset is the stored map, unchecked here (see `check_tokens`)."""
    if theme_config.palette == DEFAULT_PALETTE:
        return PaletteResult(dict(AMBER_TOKENS))
    if theme_config.palette == CUSTOM:
        assert theme_config.custom is not None  # the model requires it
        custom = theme_config.custom
        result = _resolve(custom.primary, custom.background or DEFAULT_BACKGROUND, custom.warning)
    else:
        result = _resolve(*PRESETS[theme_config.palette])
    return replace(result, tokens=dict(result.tokens), suggestion=dict(result.suggestion))


clear_palette_cache = _resolve.cache_clear


@dataclass(frozen=True)
class Theme:
    config: ThemeConfig
    tokens: dict[str, str]


def load_theme(config_dir: Path) -> Theme:
    """The team's theme with its derived tokens; ThemeError names the file and every failing
    rule when the file or its palette can't be used."""
    theme_config = load_theme_config(config_dir)
    result = resolve_palette(theme_config)
    if not result.ok:
        raise ThemeError(f"{THEME_FILE}: {result.message}")
    return Theme(theme_config, result.tokens)
