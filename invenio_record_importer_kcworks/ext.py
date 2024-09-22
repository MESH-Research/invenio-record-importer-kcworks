# -*- coding: utf-8 -*-
#
# This file is part of the invenio-record-importer-kcworks package.
# Copyright (C) 2024, Mesh Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

import logging
from .config import ImporterConfig


class InvenioRecordImporter(object):
    """Flask extension for invenio-record-importer-kcworks.

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
        app.extensions["invenio-record-importer-kcworks"] = self

    def init_config(self, app) -> None:
        """Initialize configuration for the extention.

        Args:
            app (Flask): the Flask application object on which to initialize
                the extension
        """
        self.config = ImporterConfig(app)
        for k in dir(self.config):
            if k.startswith("RECORD_IMPORTER_"):
                app.config.setdefault(k, getattr(self.config, k))

        # print("init_config")
        # print(app.logger)
        # print(app.logger.handlers)
        # print(app.logger.handlers[0])
        # app.logger.handlers[0].setFormatter(
        #     logging.Formatter(
        #         "[%(asctime)s] %(levelname)s - %(message)s "
        #         "{%(filename)s:%(lineno)d}",
        #         "%m-%d %H:%M:%S",
        #     )
        # )
