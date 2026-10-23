"""Flashpoint: FRC robot telemetry platform."""

from importlib import metadata

# The release workflow sets the package version from the tag; this reads it back.
__version__ = metadata.version("flashpoint")
