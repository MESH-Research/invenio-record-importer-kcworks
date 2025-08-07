# -*- coding: utf-8 -*-
#
# This file is part of the invenio-record-importer-kcworks package.
# Copyright (C) 2024, Mesh Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

from .config import ConfigVariables, RecordImporterServiceConfig
from .resources import RecordImporterResource, RecordImporterResourceConfig
from .service import RecordImporterService


class InvenioRecordImporter(object):
    """Flask extension for invenio-record-importer-kcworks.

    Args:
        object (_type_): _description_
    """

    def __init__(self, app=None, **kwargs) -> None:
        """Extention initialization."""
        if app:
            self._state = self.init_app(app, **kwargs)

    def init_app(self, app, **kwargs) -> None:
        """Registers the Flask extension during app initialization.

        Args:
            app (Flask): the Flask application object on which to initialize
                the extension
        """
        self.init_config(app)
        self.init_service(app)
        self.init_resources(app)
        app.extensions["invenio-record-importer-kcworks"] = self

    def init_config(self, app) -> None:
        """Initialize configuration for the extention.

        Args:
            app (Flask): the Flask application object on which to initialize
                the extension
        """
        config_vars = ConfigVariables(app)
        for k in dir(config_vars):
            if k.startswith("RECORD_IMPORTER_"):
                app.config.setdefault(k, getattr(config_vars, k))

    def init_service(self, app) -> None:
        """Initialize service."""
        self.service = RecordImporterService(RecordImporterServiceConfig.build(app))

    def init_resources(self, app) -> None:
        """Initialize resources."""
        self.resource = RecordImporterResource(
            RecordImporterResourceConfig(), service=self.service
        )
