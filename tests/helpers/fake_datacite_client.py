# Borrowed from invenio-rdm-records
#
# Copyright (C) 2022 Northwestern University.
#
# Invenio-RDM-Records is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""DataCite DOI Client."""

from unittest.mock import Mock

from idutils import normalize_doi
from invenio_rdm_records.services.pids import providers


class FakeDataCiteRESTClient:
    """DataCite REST API client wrapper."""

    def __init__(
        self,
        username,
        password,
        prefix,
        test_mode=False,
        url=None,
        timeout=None,
    ):
        """Initialize the REST client wrapper.

        Args:
            username: DataCite username.
            password: DataCite password.
            prefix: DOI prefix (or CFG_DATACITE_DOI_PREFIX).
            test_mode: Use test URL when True.
            url: DataCite API base URL.
            timeout: Connect and read timeout in seconds. Specify a tuple
                (connect, read) to specify each timeout individually.
        """
        self.username = str(username)
        self.password = str(password)
        self.prefix = str(prefix)

        if test_mode:
            self.api_url = "https://api.test.datacite.org/"
        else:
            self.api_url = url or "https://api.datacite.org/"

        if not self.api_url.endswith("/"):
            self.api_url += "/"

        self.timeout = timeout

    def public_doi(self, metadata, url, doi=None):
        """Create a public doi ... not.

        Args:
            metadata: JSON format of the metadata.
            url: URL where the doi will resolve.
            doi: DOI (e.g. 10.123/456).

        Returns:
            Mock: A no-op mock standing in for the DataCite response.
        """
        return Mock()

    def update_doi(self, doi, metadata=None, url=None):
        """Update the metadata or url for a DOI ... not.

        Args:
            doi: DOI to update.
            metadata: JSON format of the metadata.
            url: URL where the doi will resolve.

        Returns:
            Mock: A no-op mock standing in for the DataCite response.
        """
        return Mock()

    def delete_doi(self, doi):
        """Delete a doi ... not.

        This will only work for draft dois.

        Args:
            doi: DOI (e.g. 10.123/456).

        Returns:
            Mock: A no-op mock standing in for the DataCite response.
        """
        return Mock()

    def hide_doi(self, doi):
        """Hide a previously registered DOI ... not.

        This DOI will no longer be found in DataCite Search.

        Args:
            doi: DOI to hide e.g. 10.12345/1.

        Returns:
            Mock: A no-op mock standing in for the DataCite response.
        """
        return Mock()

    def show_doi(self, doi):
        """Show a previously hidden DOI ... not.

        This DOI will no longer be found in DataCite Search.

        Args:
            doi: DOI to show e.g. 10.12345/1.

        Returns:
            Mock: A no-op mock standing in for the DataCite response.
        """
        return Mock()

    def check_doi(self, doi):
        """Check doi structure.

        Check that the doi has a form 12.12345/123 with the prefix defined.

        Args:
            doi: DOI string to validate (with or without prefix).

        Returns:
            str: The normalized DOI.

        Raises:
            ValueError: If the DOI prefix does not match this client's prefix.
        """
        # If prefix is in doi
        if "/" in doi:
            split = doi.split("/")
            prefix = split[0]
            if prefix != self.prefix:
                # Provided a DOI with the wrong prefix
                raise ValueError(
                    f"Wrong DOI {prefix} prefix provided, it should be "
                    f"{self.prefix} as defined in the rest client"
                )
        else:
            doi = f"{self.prefix}/{doi}"
        return normalize_doi(doi)

    def __repr__(self):
        """Create string representation of object.

        Returns:
            str: Debug representation including the username.
        """
        return f"<FakeDataCiteRESTClient: {self.username}>"


class FakeDataCiteClient(providers.DataCiteClient):
    """Fake DataCite Client."""

    @property
    def api(self):
        """DataCite REST API client instance.

        Returns:
            FakeDataCiteRESTClient: Lazily constructed fake REST client.
        """
        if self._api is None:
            self.check_credentials()
            self._api = FakeDataCiteRESTClient(
                self.cfg("username"),
                self.cfg("password"),
                self.cfg("prefix"),
                self.cfg("test_mode", True),
            )
        return self._api
