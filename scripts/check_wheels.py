"""Verify each wheel installs independently, including its PEP 561 marker."""

from __future__ import annotations

import argparse
import email
import subprocess
import tomllib
import zipfile
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--python", default="3.11")
    parser.add_argument(
        "--minimum-tests",
        action="store_true",
        help="also run the suite on wheels with minimum runtime dependencies",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    directory = root / "dist"
    workspace = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    wheels = []
    for member in workspace["tool"]["uv"]["workspace"]["members"]:
        metadata = tomllib.loads((root / member / "pyproject.toml").read_text(encoding="utf-8"))
        name = metadata["project"]["name"].replace("-", "_")
        version = metadata["project"]["version"]
        matches = list(directory.glob(f"{name}-{version}-*.whl"))
        if len(matches) != 1:
            raise RuntimeError(
                f"expected one current wheel for {name}; run uv build --all-packages"
            )
        wheels.extend(matches)
    modules = ["tix_config", "tix_asyncio", "tix_transition", "tix_lifecycle"]
    for wheel in wheels:
        with zipfile.ZipFile(wheel) as archive:
            metadata_path = next(name for name in archive.namelist() if name.endswith("/METADATA"))
            metadata = email.message_from_bytes(archive.read(metadata_path))
            module = metadata["Name"].replace("-", "_")
            assert f"{module}/py.typed" in archive.namelist(), "missing typing marker"
            assert all(
                not value.startswith("tix-") for value in metadata.get_all("Requires-Dist", [])
            ), "cross-package coupling"
        code = (
            "import importlib, importlib.util; "
            f"package = importlib.import_module({module!r}); "
            "assert package.__all__; "
            f"others = {[name for name in modules if name != module]!r}; "
            "assert all(importlib.util.find_spec(name) is None for name in others); "
            f"print('Independent wheel verified: {module}')"
        )
        subprocess.run(
            [
                "uv",
                "run",
                "--no-project",
                "--isolated",
                "--python",
                args.python,
                "--with",
                str(wheel),
                "python",
                "-c",
                code,
            ],
            check=True,
        )

    if args.minimum_tests:
        command = ["uv", "run", "--no-project", "--isolated", "--python", args.python]
        requirements = [str(wheel) for wheel in wheels] + [
            "anyio==4.4.0",
            "transitions==0.9.2",
            "pytest>=8,<10",
            "trio>=0.27,<1",
            "pyyaml>=6,<7",
        ]
        for requirement in requirements:
            command.extend(("--with", requirement))
        command.extend(("python", "-m", "pytest", str(root / "tests"), "-q"))
        subprocess.run(command, cwd=root, check=True)


if __name__ == "__main__":
    main()
