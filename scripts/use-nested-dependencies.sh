#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2025 MESH Research
#
# Pre-install script: modifies pyproject.toml to use GitHub source for invenio-group-collections-kcworks
# if the peer directory doesn't exist. This allows the package to work both in knowledge-commons-works
# (where peer exists) and standalone (where it doesn't).
#
# Usage: ./scripts/use-nested-dependencies.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYPROJECT_TOML="${PROJECT_ROOT}/pyproject.toml"
PEER_DIR="${PROJECT_ROOT}/../invenio-group-collections-kcworks"

# Check if pyproject.toml exists
if [[ ! -f "${PYPROJECT_TOML}" ]]; then
    echo "Error: pyproject.toml not found at ${PYPROJECT_TOML}" >&2
    exit 1
fi

# Check if peer directory exists
if [[ -d "${PEER_DIR}" ]]; then
    echo "Peer directory found at ${PEER_DIR}, keeping local path in pyproject.toml"
    exit 0
fi

echo "Peer directory not found at ${PEER_DIR}, updating pyproject.toml to use GitHub source"

# Use Python to modify the TOML file properly
python3 <<EOF
import tomli
import tomli_w

# Read the existing pyproject.toml
with open("${PYPROJECT_TOML}", "rb") as f:
    data = tomli.load(f)

# Modify the source to use GitHub
if "tool" in data and "uv" in data["tool"] and "sources" in data["tool"]["uv"]:
    data["tool"]["uv"]["sources"]["invenio-group-collections-kcworks"] = {
        "git": "https://github.com/MESH-Research/invenio-group-collections-kcworks.git",
        "branch": "main"
    }

# Write back to file
with open("${PYPROJECT_TOML}", "wb") as f:
    tomli_w.dump(data, f)

print("Successfully updated pyproject.toml to use GitHub source for invenio-group-collections-kcworks")
EOF

