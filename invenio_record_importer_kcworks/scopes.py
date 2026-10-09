# Part of Invenio-Record-Importer-KCWorks
# Copyright (C) 2023-2026, MESH Research
#
# Invenio-Record-Importer-KCWorks is free software; you can redistribute and/or
# modify it under the terms of the MIT License; see LICENSE file for more details.

"""OAuth scope for the bulk import API.

Registered via `invenio_oauth2server.scopes` entry point
`records_import` in this package's `pyproject.toml`.
"""

from invenio_i18n import lazy_gettext as _
from invenio_oauth2server.models import Scope

records_import_scope = Scope(
    id_="records:import",
    group="records",
    help_text=_("Import records using the bulk import API."),
)
