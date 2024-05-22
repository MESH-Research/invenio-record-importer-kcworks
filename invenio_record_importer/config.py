#! /usr/bin/env python

import os
from pathlib import Path

GLOBAL_DEBUG = False
if os.getenv("MIGRATION_SERVER_DATA_DIR"):
    DATA_DIR = Path(os.getenv["MIGRATION_SERVER_DATA_DIR"])
else:
    DATA_DIR = Path(Path(__file__).parents[2], "kcr-untracked-files")
if os.getenv("MIGRATION_SERVER_FILES_LOCATION"):
    FILES_LOCATION = Path(os.environ["MIGRATION_SERVER_FILES_LOCATION"])
else:
    FILES_LOCATION = DATA_DIR / "humcore"
try:
    SERVER_DOMAIN = os.getenv("MIGRATION_SERVER_DOMAIN", "localhost")
    SERVER_PROTOCOL = os.getenv("MIGRATION_SERVER_PROTOCOL", "https")
except KeyError as e:
    print(f"Missing environment variable: {e}")
    exit(1)

# Allow this to be unset for injection during testing
API_TOKEN = os.getenv("MIGRATION_API_TOKEN")
