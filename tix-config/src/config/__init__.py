"""Composable local configuration without a required validation framework."""

from .manager import ConfigManager, Reload, SourceError, save_json
from .merge import deep_merge, resolve_references
from .sources import (
    CallableSource,
    ConfigSource,
    EnvSource,
    FileSource,
    MappingSource,
    SecretDirectorySource,
)
from .values import MISSING, Change, Secret, Snapshot

__all__ = [
    "MISSING",
    "CallableSource",
    "Change",
    "ConfigManager",
    "ConfigSource",
    "EnvSource",
    "FileSource",
    "MappingSource",
    "Reload",
    "Secret",
    "SecretDirectorySource",
    "Snapshot",
    "SourceError",
    "deep_merge",
    "resolve_references",
    "save_json",
]
