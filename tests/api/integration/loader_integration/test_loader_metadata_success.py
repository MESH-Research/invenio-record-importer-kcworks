# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for the API record import."""

import copy
import re

from flask_login import login_user
from invenio_access.permissions import authenticated_user
from invenio_access.utils import get_identity
from invenio_accounts.proxies import current_accounts

from invenio_record_importer_kcworks.record_loader import RecordLoader
from invenio_record_importer_kcworks.types import LoaderResult
from tests.fixtures.records import TestRecordMetadata
from tests.helpers.sample_records import (
    sample_metadata_journal_article_pdf,
)

# TODO: Add tests for the other sample records
# sample_metadata_chapter3_pdf,
# sample_metadata_chapter4_pdf,
# sample_metadata_chapter5_pdf,
# sample_metadata_conference_proceedings_pdf,
# sample_metadata_interview_transcript_pdf,
# sample_metadata_thesis_pdf,
# sample_metadata_white_paper_pdf,


class LoaderTest:
    """Base class for testing record imports with different metadata sources."""

    @property
    def metadata_source(self):
        """Override this in subclasses to provide specific metadata."""
        raise NotImplementedError

    def modify_metadata(self, test_metadata: TestRecordMetadata):
        """Modify the metadata in the metadata source class instance."""
        pass

    def check_result_status(self, result: LoaderResult):
        """Check the status of the result."""
        assert result.status == "new_record"

    def check_result_record_created(
        self, result: LoaderResult, test_metadata: TestRecordMetadata
    ):
        """Do the comparison of the result with the expected metadata."""
        assert test_metadata.compare_published(result.record_created["record_data"])
        assert result.record_created["record_data"]["revision_id"] == 4

        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            str(result.record_created["record_uuid"]),
        )
        assert result.record_created["status"] == "new_record"

    def check_result_primary_community(self, result: LoaderResult, community: dict):
        """Check the primary community of the result."""
        assert result.primary_community["id"] == community["id"]
        assert result.primary_community["metadata"]["title"] == "My Community"
        assert result.primary_community["slug"] == "neh"

    def check_result_existing_record(self, result: LoaderResult):
        """Check the existing record of the result."""
        assert result.existing_record == {}

    def check_result_uploaded_files(self, result: LoaderResult):
        """Check the uploaded files of the result."""
        assert result.uploaded_files == {}

    def check_result_community_review_result(
        self, result: LoaderResult, community: dict, test_metadata: TestRecordMetadata
    ):
        """Check the community review result of the result."""
        assert result.community_review_result["is_closed"]
        assert not result.community_review_result["is_expired"]
        assert not result.community_review_result["is_open"]
        assert (
            result.community_review_result["receiver"]["community"] == community["id"]
        )
        assert result.community_review_result["revision_id"] == 4
        assert result.community_review_result["status"] == "accepted"
        assert (
            result.community_review_result["title"]
            == test_metadata.metadata_in["metadata"]["title"]
        )
        assert (
            result.community_review_result["topic"]["record"]
            == result.record_created["record_data"]["id"]
        )
        assert result.community_review_result["type"] == "community-submission"

    def check_result_assigned_owners(
        self,
        result: LoaderResult,
        user_id: str,
        test_metadata: TestRecordMetadata,
        app,
        mailbox,
    ):
        """Check the assigned owners of the result."""
        owners = (
            test_metadata.metadata_in
            .get("parent", {})
            .get("access", {})
            .get("owned_by")
        )
        if owners and result.status == "new_record":
            owners = [
                current_accounts.datastore.get_user_by_email(owner["email"])
                for owner in owners
            ]
            assert result.assigned_owners == {
                "owner_email": owners[0].email,
                "owner_id": owners[0].id,
                "owner_type": "user",
                "access_grants": [
                    {
                        "subject": {
                            "id": str(owner.id),
                            "type": "user",
                            "email": owner.email,
                        },
                        "permission": "manage",
                    }
                    for owner in owners[1:]
                ],
            }
            assert len(mailbox) == len(owners)
            for owner in owners:
                owner_sent_mail = [m for m in mailbox if m.recipients == [owner.email]][
                    0
                ]
                assert (
                    owner_sent_mail.subject == "Your NEH Open Access Deposit is Ready"
                )
                assert owner_sent_mail.recipients == [owner.email]
                assert owner_sent_mail.sender == app.config["MAIL_DEFAULT_SENDER"]
                assert "Your NEH Open Access Deposit is Ready" in owner_sent_mail.html
                assert "Your NEH Open Access Deposit is Ready" in owner_sent_mail.body
                assert (
                    test_metadata.metadata_in["metadata"]["title"]
                    in owner_sent_mail.html
                )
                assert (
                    test_metadata.metadata_in["metadata"]["title"]
                    in owner_sent_mail.body
                )
                assert (
                    result.record_created["record_data"]["pids"]["doi"]["identifier"]
                    in owner_sent_mail.html
                )
                assert (
                    result.record_created["record_data"]["pids"]["doi"]["identifier"]
                    in owner_sent_mail.body
                )
                assert (
                    result.record_created["record_data"]["links"]["self_html"]
                    in owner_sent_mail.html
                )
                assert (
                    result.record_created["record_data"]["links"]["self_html"]
                    in owner_sent_mail.body
                )
                assert (
                    result.primary_community["links"]["self_html"]
                    in owner_sent_mail.html
                )
                assert (
                    result.primary_community["links"]["self_html"]
                    in owner_sent_mail.body
                )

        elif result.status == "new_record":
            assert result.assigned_owners == {
                "owner_id": user_id,
                "owner_email": "test@example.com",
                "owner_type": "user",
                "access_grants": [],
            }
        else:
            assert result.assigned_owners == {}

    def check_result_added_to_collections(self, result: LoaderResult):
        """Check the added to collections of the result."""
        assert result.added_to_collections == []

    def check_result_submitted(
        self,
        result: LoaderResult,
        test_metadata: TestRecordMetadata,
        app,
    ):
        """Check the submitted of the result."""
        submitted_data = copy.deepcopy(test_metadata.metadata_in)
        # Remove the owned_by field from the access dictionary during
        # record creation because we will be adding it back in later
        if submitted_data.get("parent", {}).get("access", {}).get("owned_by"):
            submitted_data["parent"]["access"].pop("owned_by")
        # Remove the entries field from the files dictionary during
        # record creation because we will be adding it back in later
        if submitted_data.get("files", {}).get("entries"):
            submitted_data["files"].pop("entries")
        # Add an empty "access" field to the expected submitted data
        # if it wasn't present in the sample data, since it gets added
        # by the loader
        if not submitted_data.get("access"):
            submitted_data["access"] = {}
        # Remove the parent and pids fields from submitted_data for comparison
        # since they are excluded from comparison_data
        if "parent" in submitted_data:
            submitted_data.pop("parent")
        if "pids" in submitted_data:
            submitted_data.pop("pids")
        comparison_data = {
            k: v
            for k, v in result.submitted["data"].items()
            if k not in ["parent", "pids"]
        }
        assert comparison_data == submitted_data
        # The test sometimes adds checksums and ids to the input file list
        # so we need to remove them for the comparison
        submitted_files = copy.deepcopy(test_metadata.metadata_in["files"])
        if submitted_files.get("entries"):
            submitted_files["entries"] = {
                k: {k: v for k, v in v.items() if k != "checksum" and k != "id"}
                for k, v in submitted_files["entries"].items()
            }
        assert result.submitted["files"] == submitted_files
        assert result.submitted["owners"] == test_metadata.metadata_in.get(
            "parent", {}
        ).get("access", {}).get("owned_by", [])

    def check_result_errors(self, result: LoaderResult):
        """Check the errors of the result."""
        assert result.errors == []

    def test_import_records_loader_load(
        self,
        db,
        running_app,
        search_clear,
        minimal_community_factory,
        user_factory,
        record_metadata,
        mock_send_remote_api_update_fixture,
        celery_worker,
        mailbox,
        *args,
    ):
        """Test that a record can be imported via the API."""
        app = running_app.app

        # Get the email of the first owner of the record if owners are specified
        owners = (
            self.metadata_source.get("parent", {}).get("access", {}).get("owned_by", [])
        )
        if owners:
            first_user_email = owners[0].get("email")
        else:
            first_user_email = "test@example.com"
        u = user_factory(email=first_user_email, token=True, oauth_id="")
        user_id = u.user.id
        identity = get_identity(u.user)
        identity.provides.add(authenticated_user)
        login_user(u.user)

        community_record = minimal_community_factory(owner=user_id, slug="neh")
        community = community_record.to_dict()

        # Metadata-only import: strip sample file entries before the fixture
        # sees them. With-files coverage lives in BaseImportLoaderWithFilesTest.
        metadata_in = copy.deepcopy(self.metadata_source)
        metadata_in["files"] = {"enabled": False}

        test_metadata = record_metadata(
            metadata_in=metadata_in,
            community_list=[community],
            owner_id=user_id,
        )
        test_metadata.update_metadata({
            "metadata|identifiers": [
                {"identifier": "1234567890", "scheme": "import-recid"}
            ]
        })
        self.modify_metadata(test_metadata)

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

        result: LoaderResult = RecordLoader(
            user_id=user_id, community_id=community["id"]
        ).load(
            index=0,
            import_data=copy.deepcopy(test_metadata.metadata_in),
            notify_record_owners=True,
        )

        assert result.log_object
        assert result.source_id
        self.check_result_submitted(result, test_metadata, app)
        self.check_result_record_created(result, test_metadata)
        self.check_result_status(result)
        self.check_result_primary_community(result, community)
        self.check_result_existing_record(result)
        self.check_result_uploaded_files(result)

        community.update({"links": {}})  # FIXME: Why are links not expanded?

        self.check_result_community_review_result(result, community, test_metadata)
        self.check_result_assigned_owners(result, user_id, test_metadata, app, mailbox)
        self.check_result_added_to_collections(result)
        self.check_result_errors(result)


# class TestImportLoaderLoadThesisPDF(BaseImportRecordsLoaderLoadTest):
#     @property
#     def metadata_source(self):
#         return sample_metadata_thesis_pdf


# class TestImportLoaderLoadChapterPDF(BaseImportRecordsLoaderLoadTest):
#     @property
#     def metadata_source(self):
#         return sample_metadata_chapter_pdf


# class TestImportLoaderLoadChapter2PDF(BaseImportRecordsLoaderLoadTest):
#     @property
#     def metadata_source(self):
#         return sample_metadata_chapter2_pdf


class TestLoaderJArticle(LoaderTest):
    """Test importing a journal article."""

    @property
    def metadata_source(self):  # noqa: D102
        return copy.deepcopy(sample_metadata_journal_article_pdf)
