#!/usr/bin/env python3
"""Docs screenshots and animations from `docs/media-manifest.toml` (mvp-documentation, D-f).

    python tools/docs-media.py placeholders [--out DIR]
    python tools/docs-media.py check-site SITE_DIR

`placeholders` writes a tiny valid file for every manifest entry (a PNG per screenshot, a WebM
and a GIF per animation) so the docs site builds without the corpus. `check-site` fails when a
built page shows an image or video whose file isn't in the built site: Zensical's strict build
doesn't check media (ADR-0019). `capture` (the real images) comes later.
"""

import argparse
import re
import struct
import sys
import tomllib
import zlib
from pathlib import Path
from urllib.parse import unquote, urlsplit

REPO = Path(__file__).resolve().parents[1]
MANIFEST = REPO / "docs" / "media-manifest.toml"
MEDIA = REPO / "docs" / "guide" / "assets" / "media"
SUFFIXES = {"screenshot": (".png",), "animation": (".webm", ".gif")}

# A 1x1 transparent GIF89a.
GIF = bytes.fromhex(
    "474946383961010001008000000000000000ff21f90401000000002c00000000010001000002024401003b"
)


def load_manifest(path: Path = MANIFEST) -> list[dict[str, str]]:
    shots: list[dict[str, str]] = tomllib.loads(path.read_text(encoding="utf-8"))["shot"]
    return shots


def file_names(shots: list[dict[str, str]]) -> list[str]:
    """Every file the manifest's entries produce, in manifest order."""
    return [shot["id"] + suffix for shot in shots for suffix in SUFFIXES[shot["kind"]]]


def png() -> bytes:
    """A 1x1 transparent PNG."""

    def chunk(kind: bytes, data: bytes) -> bytes:
        crc = struct.pack(">I", zlib.crc32(kind + data))
        return struct.pack(">I", len(data)) + kind + data + crc

    header = struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0)  # 1x1, 8-bit RGBA
    pixels = zlib.compress(b"\x00\x00\x00\x00\x00")  # filter byte + one transparent pixel
    chunks = chunk(b"IHDR", header) + chunk(b"IDAT", pixels) + chunk(b"IEND", b"")
    return b"\x89PNG\r\n\x1a\n" + chunks


# One black 16x16 VP8 frame (made once with `ffmpeg -f lavfi -i color=c=black:s=16x16 -frames:v 1
# -c:v libvpx`); browsers play it, unlike a WebM with no frames.
WEBM = bytes.fromhex(
    "1a45dfa39f4286810142f7810142f2810442f381084282847765626d42878102428581021853806701000000"
    "000001ab114d9b74ba4dbb8b53ab841549a96653ac81a14dbb8b53ab841654ae6b53ac81c64dbb8c53ab8412"
    "54c36753ac8201134dbb8c53ab841c53bb6b53ac820195ec0100000000000059000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000001549a966a02ad7b1830f42"
    "404d80844c6176665741844c61766644898840440000000000001654ae6bc8ae010000000000003fd7810173"
    "c58800000000000000019c810022b59c83756e648881008685565f56503883810123e3838402625a00e090b0"
    "8110ba81109a810255b08455b981011254c367d07373cd63c08b63c588000000000000000167c89845a38745"
    "4e434f44455244878b4c617663206c696276707867c8a145a3884455524154494f4e44879330303a30303a30"
    "302e303430303030303030001f43b675a8e78100a3a3810000801002009d012a100010000047088585888584"
    "880202000c0d6000feffab50801c53bb6b91bb8fb38100b78af78101f1820168f08103"
)


MEDIA_SRC = re.compile(r"<(?:img|video|source)\b[^>]*?\ssrc=[\"']([^\"']+)[\"']", re.IGNORECASE)


def missing_media(site: Path) -> list[str]:
    """Every local image or video a built page shows whose file isn't in the site."""
    problems = []
    for page in sorted(site.rglob("*.html")):
        for source in MEDIA_SRC.findall(page.read_text(encoding="utf-8")):
            parts = urlsplit(source)
            if parts.scheme or parts.netloc or source.startswith("data:"):
                continue
            base = site if parts.path.startswith("/") else page.parent
            if not (base / unquote(parts.path).lstrip("/")).is_file():
                problems.append(f"{page.relative_to(site).as_posix()}: missing {source}")
    return problems


def content(suffix: str) -> bytes:
    """The placeholder bytes for one media file type."""
    return {".png": png(), ".gif": GIF, ".webm": WEBM}[suffix]


def placeholders(out: Path, manifest: Path = MANIFEST) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name in file_names(load_manifest(manifest)):
        target = out / name
        target.write_bytes(content(target.suffix))
        written.append(target)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    holders = commands.add_parser("placeholders", help="write a tiny stand-in for every entry")
    holders.add_argument("--out", type=Path, default=MEDIA, help=f"default: {MEDIA}")
    check = commands.add_parser("check-site", help="fail on media a built page shows but lacks")
    check.add_argument("site", type=Path, help="the built site folder")
    args = parser.parse_args(argv)
    if args.command == "check-site":
        problems = missing_media(args.site)
        for problem in problems:
            print(problem, file=sys.stderr)
        print(f"{len(problems)} missing media files in {args.site}")
        return 1 if problems else 0
    written = placeholders(args.out)
    print(f"{len(written)} placeholders in {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
