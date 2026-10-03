"""Type-check source directories under their installed package names."""

from __future__ import annotations

import tomllib
from pathlib import Path

from mypy.build import build
from mypy.errors import CompileError
from mypy.main import process_options
from mypy.modulefinder import BuildSource


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    _, options = process_options(["--config-file", str(root / "pyproject.toml")])
    workspace = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    sources = []
    for member in workspace["tool"]["uv"]["workspace"]["members"]:
        project = root / member
        metadata = tomllib.loads((project / "pyproject.toml").read_text(encoding="utf-8"))
        for package, directory in metadata["tool"]["setuptools"]["package-dir"].items():
            source_root = project / directory
            for path in sorted(source_root.rglob("*.py")):
                parts = list(path.relative_to(source_root).with_suffix("").parts)
                if parts[-1] == "__init__":
                    parts.pop()
                sources.append(BuildSource(str(path), ".".join([package, *parts]), None))
    try:
        result = build(sources, options)
    except CompileError as error:
        print("\n".join(error.messages))
        return 1
    if result.errors:
        print("\n".join(result.errors))
        return 1
    print(f"Success: no issues found in {len(sources)} source files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
