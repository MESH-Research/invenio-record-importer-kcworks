#! /usr/bin/env python

import os
from pathlib import Path

GLOBAL_DEBUG = False
if os.environ.get("MIGRATION_SERVER_DATA_DIR"):
    DATA_DIR = Path(os.environ["MIGRATION_SERVER_DATA_DIR"])
else:
    DATA_DIR = Path(Path(__file__).parents[2], "kcr-untracked-files")
if os.environ.get("MIGRATION_SERVER_FILES_LOCATION"):
    FILES_LOCATION = Path(os.environ["MIGRATION_SERVER_FILES_LOCATION"])
else:
    FILES_LOCATION = DATA_DIR / "humcore"
SERVER_DOMAIN = os.environ["MIGRATION_SERVER_DOMAIN"]
SERVER_PROTOCOL = os.environ["MIGRATION_SERVER_PROTOCOL"]
API_TOKEN = os.environ["MIGRATION_API_TOKEN"]
