#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2025 MESH Research
#
# Pre-install script: modifies pyproject.toml to use GitHub source for peer dependencies
# if the peer directories don't exist. This allows the package to work both in knowledge-commons-works
# (where peers exist) and standalone (where they don't).
#
# Usage: ./scripts/use-nested-dependencies.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYPROJECT_TOML="${PROJECT_ROOT}/pyproject.toml"
PEER_KCWORKS="${PROJECT_ROOT}/../../../../../knowledge-commons-works"
PEER_COMMUNITIES="${PROJECT_ROOT}/../invenio-communities"
PEER_GROUP_COLLECTIONS="${PROJECT_ROOT}/../invenio-group-collections-kcworks"
PEER_REMOTE_USER_DATA="${PROJECT_ROOT}/../invenio-remote-user-data-kcworks"
PEER_STATS_DASHBOARD="${PROJECT_ROOT}/../invenio-stats-dashboard"

# Check if pyproject.toml exists
if [[ ! -f "${PYPROJECT_TOML}" ]]; then
    echo "Error: pyproject.toml not found at ${PYPROJECT_TOML}" >&2
    exit 1
fi

# Check which peer directories exist
NEEDS_UPDATE=0
if [[ ! -d "${PEER_KCWORKS}" ]]; then
    echo "Peer directory not found at ${PEER_KCWORKS}, will use GitHub source"
    NEEDS_UPDATE=1
fi

if [[ ! -d "${PEER_COMMUNITIES}" ]]; then
    echo "Peer directory not found at ${PEER_COMMUNITIES}, will use GitHub source"
    NEEDS_UPDATE=1
fi

if [[ ! -d "${PEER_GROUP_COLLECTIONS}" ]]; then
    echo "Peer directory not found at ${PEER_GROUP_COLLECTIONS}, will use GitHub source"
    NEEDS_UPDATE=1
fi

if [[ ! -d "${PEER_REMOTE_USER_DATA}" ]]; then
    echo "Peer directory not found at ${PEER_REMOTE_USER_DATA}, will use GitHub source"
    NEEDS_UPDATE=1
fi

if [[ ! -d "${PEER_STATS_DASHBOARD}" ]]; then
    echo "Peer directory not found at ${PEER_STATS_DASHBOARD}, will use GitHub source"
    NEEDS_UPDATE=1
fi

if [[ ${NEEDS_UPDATE} -eq 0 ]]; then
    echo "Peer directories found, keeping local paths in pyproject.toml"
    exit 0
fi

echo "Updating pyproject.toml to use GitHub sources for missing peer dependencies"

# Use Python to modify the TOML file properly
python3 <<EOF
import tomli
import tomli_w
import os

# Read the existing pyproject.toml
with open("${PYPROJECT_TOML}", "rb") as f:
    data = tomli.load(f)

# Modify the sources to use GitHub if peers don't exist
if "tool" in data and "uv" in data["tool"] and "sources" in data["tool"]["uv"]:
    if not os.path.exists("${PEER_KCWORKS}"):
        data["tool"]["uv"]["sources"]["kcworks"] = {
            "git": "https://github.com/MESH-Research/knowledge-commons-works.git",
            "branch": "main",
            "subdirectory": "site"
        }
    
    if not os.path.exists("${PEER_COMMUNITIES}"):
        data["tool"]["uv"]["sources"]["invenio-communities"] = {
            "git": "https://github.com/MESH-Research/invenio-communities.git",
            "branch": "local-working"
        }
    
    if not os.path.exists("${PEER_GROUP_COLLECTIONS}"):
        data["tool"]["uv"]["sources"]["invenio-group-collections-kcworks"] = {
            "git": "https://github.com/MESH-Research/invenio-group-collections-kcworks.git",
            "branch": "main"
        }
    
    if not os.path.exists("${PEER_REMOTE_USER_DATA}"):
        data["tool"]["uv"]["sources"]["invenio-remote-user-data-kcworks"] = {
            "git": "https://github.com/MESH-Research/invenio-remote-user-data-kcworks.git",
            "branch": "main"
        }
    
    if not os.path.exists("${PEER_STATS_DASHBOARD}"):
        data["tool"]["uv"]["sources"]["invenio-stats-dashboard"] = {
            "git": "https://github.com/MESH-Research/invenio-stats-dashboard.git",
            "branch": "main"
        }

# Write back to file
with open("${PYPROJECT_TOML}", "wb") as f:
    tomli_w.dump(data, f)

print("Successfully updated pyproject.toml to use GitHub sources for peer dependencies")
EOF

