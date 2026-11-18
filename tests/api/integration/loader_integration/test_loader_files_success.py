# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for successful RecordLoader.load() with files."""

import copy
import sys

from flask_login import login_user
from invenio_access.permissions import authenticated_user, system_identity
from invenio_access.utils import get_identity
from invenio_rdm_records.proxies import current_rdm_records_service as records_service
from invenio_vocabularies.proxies import current_service as current_vocabulary_service
from invenio_vocabularies.records.api import Vocabulary

from invenio_record_importer_kcworks.record_loader import RecordLoader
from invenio_record_importer_kcworks.types import FileData, LoaderResult
from tests.fixtures.files import file_md5
from tests.fixtures.records import TestRecordMetadata
from tests.helpers.sample_records import (
    sample_metadata_journal_article_pdf,
)

from .test_loader_metadata_success import LoaderTest


class LoaderFilesTest(LoaderTest):
    """Base class for testing record imports with files."""

    def check_result_uploaded_files(self, result: LoaderResult):
        """Check the uploaded files of the result."""
        assert result.uploaded_files == {
            "sample.jpg": {"status": "uploaded", "messages": []},
            "sample.pdf": {"status": "uploaded", "messages": []},
        }

    def check_result_record_created(
        self, result: LoaderResult, test_metadata: TestRecordMetadata
    ):
        """Check the record created of the result."""
        assert test_metadata.compare_published(result.record_created["record_data"])
        # assert result.record_created["record_data"]["revision_id"] == 4
        # FIXME: sometimes 3, sometimes 4

    def test_import_records_loader_load(
        self,
        db,
        running_app,
        search_clear,
        minimal_community_factory,
        user_factory,
        record_metadata_with_files,
        mock_send_remote_api_update_fixture,
        celery_worker,
        mailbox,
        test_sample_files_folder,
        reindex_resource_types,
        reindex_date_types,
    ):
        """Test importing a record with files."""
        app = running_app.app
        self.test_sample_files_folder = test_sample_files_folder

        # find the resource type id for "textDocument"
        rt = current_vocabulary_service.read(
            system_identity,
            id_=("resourcetypes", "textDocument-journalArticle"),
        )

        Vocabulary.index.refresh()
        # Search for all resourcetypes
        search_result = current_vocabulary_service.search(
            system_identity,
            type="resourcetypes",
        )

        # Get the hits from the search result
        resource_types = search_result.to_dict()["hits"]["hits"]

        # Print each resource type
        for rt in resource_types:
            app.logger.debug(
                f"resource type: ID: {rt['id']}, Title: {rt['title']['en']}"
            )

        u = user_factory(email="test@example.com", token=True, oauth_id=None)
        user_id = u.user.id
        identity = get_identity(u.user)
        identity.provides.add(authenticated_user)
        login_user(u.user)

        community_record = minimal_community_factory(owner=user_id, slug="neh")
        community = community_record.to_dict()

        file_paths = [
            test_sample_files_folder / "sample.pdf",
            test_sample_files_folder / "sample.jpg",
        ]
        file1 = open(file_paths[0], "rb")
        file2 = open(file_paths[1], "rb")
        files = [
            FileData(
                filename=str(file_paths[0]),
                stream=file1,
                content_type="application/pdf",
                mimetype="application/pdf",
                mimetype_params={},
            ),
            FileData(
                filename=str(file_paths[1]),
                stream=file2,
                content_type="image/jpeg",
                mimetype="image/jpeg",
                mimetype_params={},
            ),
        ]
        file_list = [
            {
                "key": "sample.pdf",
                "mimetype": "application/pdf",
                "size": 13264,  # FIXME: Check reporting of mismatch
            },
            {
                "key": "sample.jpg",
                "mimetype": "image/jpeg",
                "size": 1174188,
            },
        ]
        file_entries = {f["key"]: f for f in file_list}

        test_metadata = record_metadata_with_files(
            metadata_in=self.metadata_source,
            community_list=[community],
            owner_id=user_id,
            file_entries=file_entries,
        )
        test_metadata.update_metadata({
            "metadata|identifiers": [
                {"identifier": "hc:33383", "scheme": "import-recid"}
            ]
        })
        for u in (
            test_metadata.metadata_in
            .get("parent", {})
            .get("access", {})
            .get("owned_by", [])
        ):
            if u["email"] != "test@example.com":
                user_factory(
                    email=u["email"],
                    token=False,
                    oauth_id=None,
                )

        # Create group communities
        for g in test_metadata.metadata_in.get("custom_fields", {}).get(
            "hclegacy:groups_for_deposit", []
        ):
            minimal_community_factory(
                slug=g["group_name"].lower().replace(" ", "-"),
                custom_fields={
                    "kcr:commons_group_id": g["group_identifier"],
                    "kcr:commons_group_name": g["group_name"],
                },
            )

        result: LoaderResult = RecordLoader(
            user_id=user_id, community_id=community["id"]
        ).load(
            index=0,
            import_data=copy.deepcopy(test_metadata.metadata_in),
            files=files,
            notify_record_owners=True,
        )
        file1.close()
        file2.close()

        record_created_id = result.record_created["record_data"]["id"]

        # add ids and checksums from actual file entries to the expected file entries
        for k, f in file_entries.items():
            f["id"] = result.record_created["record_data"]["files"]["entries"][k]["id"]
            f["checksum"] = result.record_created["record_data"]["files"]["entries"][k][
                "checksum"
            ]
        test_metadata.file_entries = file_entries
        test_metadata.record_id = record_created_id

        self.check_result_status(result)
        self.check_result_primary_community(result, community)
        self.check_result_existing_record(result)
        self.check_result_record_created(result, test_metadata)
        self.check_result_uploaded_files(result)
        self.check_result_community_review_result(result, community, test_metadata)
        self.check_result_assigned_owners(result, user_id, test_metadata, app, mailbox)
        self.check_result_added_to_collections(result)
        self.check_result_submitted(result, test_metadata, app)
        self.check_result_errors(result)
        assert result.log_object
        assert result.source_id

        # now check the record in the database/search
        records_service.read(system_identity, id_=record_created_id).to_dict()
        expected_record_files = copy.deepcopy(test_metadata.published["files"])

        # FIXME: There's an inconsistency in file metadata between test runs
        # locally and on github. The github run extracts image dimensions.
        if "sample.jpg" in expected_record_files["entries"].keys():
            expected_record_files["entries"]["sample.jpg"]["metadata"] = {}
        assert expected_record_files == {
            k: v for k, v in expected_record_files.items() if k != "default_preview"
        }

        # ensure the files can be downloaded
        with app.test_client() as client:
            with open(file_paths[1], "rb") as file2:
                file_bytes = file2.read()
                file_response2 = client.get(
                    f"{app.config['SITE_API_URL']}/records/{record_created_id}/files/"
                    "sample.jpg/content"
                )
                assert file_response2.status_code == 200
                assert (
                    "inline" in file_response2.headers["Content-Disposition"]
                )  # FIXME: why not attachment?
                assert file_response2.headers["Content-MD5"] == file_md5(
                    file_response2.data
                )
                assert file_response2.headers["Content-MD5"] == file_md5(file_bytes)
                assert file_response2.content_type == "image/jpeg"
                assert file_response2.content_length == 1174188
                assert sys.getsizeof(file_response2.data) == sys.getsizeof(file_bytes)
                # assert file_response2.data == file2.read()

            with open(file_paths[0], "rb") as file1:
                file_bytes = file1.read()
                file_response1 = client.get(
                    f"{app.config['SITE_API_URL']}/records/{record_created_id}/files/"
                    "sample.pdf/content"
                )
                assert file_response1.status_code == 200
                assert file_response1.headers["Content-MD5"] == file_md5(
                    file_response1.data
                )
                assert file_response1.headers["Content-MD5"] == file_md5(file_bytes)
                assert "sample.pdf" in file_response1.headers["Content-Disposition"]
                assert (
                    file_response1.content_type == "application/octet-stream"
                )  # FIXME: why not application/pdf?
                assert file_response1.content_length == 13264
                assert sys.getsizeof(file_response1.data) == sys.getsizeof(file_bytes)
                # assert file_response1.data == file1.read()

        file1.close()
        file2.close()


# class TestImportLoaderLoadWithFilesChapterPDF(BaseImportLoaderWithFilesTest):
#     @property
#     def metadata_source(self):
#         return copy.deepcopy(sample_metadata_chapter_pdf)


# class TestImportLoaderLoadWithFilesChapter2PDF(BaseImportLoaderWithFilesTest):
#     @property
#     def metadata_source(self):
#         return copy.deepcopy(sample_metadata_chapter2_pdf)


class TestLoaderFilesJArticle(LoaderFilesTest):
    """Test importing a journal article with files."""

    @property
    def metadata_source(self):  # noqa: D102
        return copy.deepcopy(sample_metadata_journal_article_pdf)
