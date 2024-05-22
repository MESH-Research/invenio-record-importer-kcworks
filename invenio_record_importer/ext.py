# -*- coding: utf-8 -*-
#
# This file is part of the invenio-record-importer package.
# Copyright (C) 2024, Mesh Research.
#
# invenio-record-importer is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.


from .config import ImporterConfig


class InvenioRecordImporter(object):
    """Flask extension for invenio-record-importer.

    Args:
        object (_type_): _description_
    """

    def __init__(self, app=None) -> None:
        """Extention initialization."""
        if app:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Registers the Flask extension during app initialization.

        Args:
            app (Flask): the Flask application object on which to initialize
                the extension
        """
        self.init_config(app)
        app.extensions["invenio-record-importer"] = self

    def init_config(self, app) -> None:
        """Initialize configuration for the extention.

        Args:
            app (Flask): the Flask application object on which to initialize
                the extension
        """
        self.config = ImporterConfig(app)
        for k in dir(self.config):
            if k.startswith("RECORD_IMPORTER_") or k.startswith("MIGRATION_"):
                app.config.setdefault(k, getattr(self.config, k))
                print(f"Setting {k} to {getattr(self.config, k)}")
