#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2026 MESH Research
#
# Pre-install script: modifies pyproject.toml to use GitHub source for peer
# dependencies if the peer directories don't exist. Allows the package to work
# both in kcworks-next (sibling peers) and standalone in CI (git sources).
#
# Usage: python scripts/use_nested_dependencies.py

from pathlib import Path
import sys

try:
    import tomli
    import tomli_w
except ImportError:
    print(
        "Error: tomli and tomli_w required. Run: uv pip install --system tomli tomli-w",
        file=sys.stderr,
    )
    sys.exit(1)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
PYPROJECT_TOML = PROJECT_ROOT / "pyproject.toml"

# Sibling paths (for monorepo); when missing we switch to git
# kcworks is at ../../../../../knowledge-commons-works from this package root
PEERS = {
    "kcworks": PROJECT_ROOT / ".." / ".." / ".." / ".." / ".." / "knowledge-commons-works",
    "invenio-communities": PROJECT_ROOT / ".." / "invenio-communities",
    "invenio-group-collections-kcworks": PROJECT_ROOT / ".." / "invenio-group-collections-kcworks",
    "invenio-remote-user-data-kcworks": PROJECT_ROOT / ".." / "invenio-remote-user-data-kcworks",
    "invenio-stats-dashboard": PROJECT_ROOT / ".." / "invenio-stats-dashboard",
}

GIT_SOURCES = {
    "kcworks": {
        "git": "https://github.com/MESH-Research/knowledge-commons-works.git",
        "branch": "main",
    },
    "invenio-communities": {
        "git": "https://github.com/MESH-Research/invenio-communities.git",
        "branch": "local-working",
    },
    "invenio-group-collections-kcworks": {
        "git": "https://github.com/MESH-Research/invenio-group-collections-kcworks.git",
        "branch": "main",
    },
    "invenio-remote-user-data-kcworks": {
        "git": "https://github.com/MESH-Research/invenio-remote-user-data-kcworks.git",
        "branch": "main",
    },
    "invenio-stats-dashboard": {
        "git": "https://github.com/MESH-Research/invenio-stats-dashboard.git",
        "branch": "main",
    },
}


def main() -> None:
    if not PYPROJECT_TOML.is_file():
        print(f"Error: pyproject.toml not found at {PYPROJECT_TOML}", file=sys.stderr)
        sys.exit(1)

    peers_missing = []
    for name, path in PEERS.items():
        if not path.resolve().is_dir():
            print(f"Peer directory not found at {path}, will use GitHub source")
            peers_missing.append(name)

    if not peers_missing:
        print("Peer directories found, keeping local paths in pyproject.toml")
        return

    print("Updating pyproject.toml to use GitHub sources for missing peer dependencies")

    with open(PYPROJECT_TOML, "rb") as f:
        data = tomli.load(f)

    if "tool" in data and "uv" in data["tool"] and "sources" in data["tool"]["uv"]:
        for name in peers_missing:
            if name in GIT_SOURCES:
                data["tool"]["uv"]["sources"][name] = GIT_SOURCES[name]

    with open(PYPROJECT_TOML, "wb") as f:
        tomli_w.dump(data, f)

    print("Successfully updated pyproject.toml to use GitHub sources for peer dependencies")


if __name__ == "__main__":
    main()
