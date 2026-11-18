# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for the RecordImporterService with success."""

import copy
import json
from pathlib import Path
from pprint import pformat

from flask_login import login_user
from invenio_access.permissions import authenticated_user, system_identity
from invenio_access.utils import get_identity
from invenio_accounts.models import User
from invenio_accounts.proxies import current_accounts
from invenio_communities.members.records.api import Member
from invenio_communities.proxies import current_communities
from invenio_communities.utils import load_community_needs
from invenio_rdm_records.proxies import current_rdm_records_service as records_service

from invenio_record_importer_kcworks.proxies import current_record_importer_service
from invenio_record_importer_kcworks.types import FileData
from tests.fixtures.records import TestRecordMetadataWithFiles
from tests.helpers.sample_records import (
    sample_metadata_journal_article2_pdf,
    sample_metadata_journal_article_pdf,
)


class ServiceTest:
    """Base class for testing RecordImporterService.import_records."""

    @property
    def by_api(self):  # noqa: D102
        return False

    @property
    def community_access_override(self):  # noqa: D102
        return {}

    def make_submitter(self, user_factory, community_id):  # noqa: D102
        return None, None

    @property
    def metadata_sources(self):
        """Override this in subclasses to provide specific metadata."""
        raise NotImplementedError

    @property
    def files_to_upload(self):
        """Override this in subclasses to provide different files to upload.

        The default defined here assumes two input records with two files each.
        """
        test_sample_files_folder = getattr(self, "test_sample_files_folder", None)
        if test_sample_files_folder is None:
            # Fallback for when fixture isn't available
            test_sample_files_folder = (
                Path(__file__).resolve().parent.parent / "helpers" / "sample_files"
            )
        file_paths = [
            test_sample_files_folder / "sample.pdf",
            test_sample_files_folder / "sample.jpg",
            test_sample_files_folder / "sample2.pdf",
            test_sample_files_folder / "sample.csv",
        ]
        file1 = open(file_paths[0], "rb")
        file2 = open(file_paths[1], "rb")
        file3 = open(file_paths[2], "rb")
        file4 = open(file_paths[3], "rb")
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
            FileData(
                filename=str(file_paths[2]),
                stream=file3,
                content_type="application/pdf",
                mimetype="application/pdf",
                mimetype_params={},
            ),
            FileData(
                filename=str(file_paths[3]),
                stream=file4,
                content_type="text/csv",
                mimetype="text/csv",
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
            {
                "key": "sample2.pdf",
                "mimetype": "application/pdf",
                "size": 13264,  # FIXME: Check reporting of mismatch
            },
            {
                "key": "sample.csv",
                "mimetype": "text/csv",
                "size": 17261,
            },
        ]
        file_streams = [file1, file2, file3, file4]
        return files, file_list, file_streams

    @property
    def expected_errors(self):
        """Override this in subclasses to provide specific expected errors.

        The expected errors should be a list of lists, where each inner list
        contains the expected errors for a record. If the record is expected to
        succeed, the inner list should be empty. The outer list should have the
        same length as the metadata sources.
        """
        return [[]] * len(self.metadata_sources)

    def check_result_status(
        self, import_results: dict, status_code: int | None
    ) -> bool:
        """Check the status of the import results.

        Returns True if the status is as expected, False otherwise.

        The boolean return is to allow for short-circuiting the main
        test execution if the status reflects a response that will lack
        a body with data or errors (e.g., 403).

        Returns:
            bool: True if status is as expected, False otherwise.
        """
        if not any(e for e in self.expected_errors if e):
            if self.by_api:
                assert status_code == 201
            assert len(import_results["data"]) == len(self.metadata_sources)
            assert import_results.get("status") == "success"
            assert (
                import_results.get("message")
                == "All records were successfully imported"
            )
        else:
            if self.by_api:
                assert status_code == 400
            assert len(import_results["data"]) == 0
            assert import_results.get("status") == "error"

            # if only some records are expected to fail
            if len([e for e in self.expected_errors if e]) < len(self.metadata_sources):
                assert import_results.get("message") == (
                    "Some records could not be imported, and the 'all_or_none' flag "
                    "was set to True, so the import was aborted and no records were "
                    "created. Please check the list of failed records in the "
                    "'errors' field for more information. Each failed item should have "
                    "its own list of specific errors."
                )
            # if all records are expected to fail
            else:
                assert import_results.get("message") == (
                    "No records were successfully imported. Please check the list of "
                    "failed records in the 'errors' field for more information. Each "
                    "failed item should have its own list of specific errors."
                )
        return True

    def _check_response_files(self, actual_files, record_files):
        assert actual_files == {
            f.filename.split("/")[-1]: {"status": "uploaded", "messages": []}
            for f in record_files
        }

    def check_result_errors(self, import_results: dict) -> None:
        """Check the errors of the import results."""
        if not any(e for e in self.expected_errors if e):
            assert import_results.get("errors") == []
            return
        error_item_indices = [
            index for index, error in enumerate(self.expected_errors) if error
        ]
        assert len(import_results["errors"]) == len(error_item_indices)
        for i, actual_error_item in enumerate(import_results["errors"]):
            assert actual_error_item["item_index"] == error_item_indices[i]
            assert (
                actual_error_item["errors"]
                == self.expected_errors[error_item_indices[i]]
            )
            if actual_error_item[
                "metadata"
            ]:  # make sure metadata present when expected
                item_recid = actual_error_item["metadata"]["id"]
                assert item_recid not in [
                    r["metadata"]["id"] for r in import_results["data"]
                ]
                created_record = records_service.read(
                    system_identity, id_=item_recid
                ).to_dict()
                assert created_record["status"] == "deleted"

    def _check_owners_in_community(
        self,
        community_members: list[Member],
        user: User,
        uploader_id: int,
    ):
        # The API submitter must keep an elevated community role. Other
        # metadata owners are normally added as reader, but import must not
        # demote someone who already holds a higher role (e.g. the community's
        # founding owner when a different hidden owner submits).
        if user.id == uploader_id:
            target_roles = ["curator", "manager", "owner"]
        else:
            target_roles = ["reader", "curator", "manager", "owner"]
        matching_ids = [m for m in community_members if m.user_id == user.id]
        assert matching_ids
        assert matching_ids[0].role in target_roles
        assert len(matching_ids) == 1

    def _check_owners(
        self,
        actual_metadata: dict,
        expected: TestRecordMetadataWithFiles,
        uploader_id: str,
        community_id: str,
        community_slug: str,
        mailbox,
        mocker,
    ):
        expected_owners = (
            expected.metadata_in.get("parent", {}).get("access", {}).get("owned_by")
        )
        if expected_owners:
            community_members = Member.get_members(community_id)
            first_expected_owner = expected.metadata_in["parent"]["access"]["owned_by"][
                0
            ]
            first_actual_owner = current_accounts.datastore.get_user_by_id(
                actual_metadata["parent"]["access"]["owned_by"]["user"]
            )
            assert first_actual_owner.email == first_expected_owner["email"]
            self._check_owners_in_community(
                community_members, first_actual_owner, int(uploader_id)
            )
            if len(expected_owners) > 1:
                other_expected_owners = expected.metadata_in["parent"]["access"][
                    "owned_by"
                ][1:]
                other_actual_owners = actual_metadata["parent"]["access"]["grants"]
                for oe, oa in zip(
                    other_expected_owners, other_actual_owners, strict=False
                ):
                    user = current_accounts.datastore.get_user_by_email(oe["email"])
                    assert oa["subject"]["id"] == str(user.id)
                    assert user.email == oe["email"]

                    if oe.get("identifiers"):
                        kc_username = next(
                            (
                                i["identifier"]
                                for i in oe["identifiers"]
                                if i["scheme"] == "kc_username"
                            ),
                            None,
                        )
                        orcid = next(
                            (
                                i["identifier"]
                                for i in oe["identifiers"]
                                if i["scheme"] == "orcid"
                            ),
                            None,
                        )
                        neh_id = next(
                            (
                                i["identifier"]
                                for i in oe["identifiers"]
                                if i["scheme"] == "neh_user_id"
                            ),
                            None,
                        )
                        import_id = next(
                            (
                                i["identifier"]
                                for i in oe["identifiers"]
                                if i["scheme"] == "import_user_id"
                            ),
                            None,
                        )
                        if kc_username:
                            assert user.username in [
                                kc_username,
                                f"knowledgeCommons-{kc_username}",
                            ]
                        if orcid:
                            assert user.user_profile["identifier_orcid"] == orcid
                        if neh_id:
                            other_user_ids = json.loads(
                                user.user_profile["identifier_other"]
                            )
                            assert neh_id in other_user_ids.values()
                        if import_id:
                            other_user_ids = json.loads(
                                user.user_profile["identifier_other"]
                            )
                            assert import_id in other_user_ids.values()
                    # make sure they were added to the community
                    # as reader (unless they are the uploader)
                    self._check_owners_in_community(
                        community_members, user, int(uploader_id)
                    )

            # multiple records created with the one mailbox
            if not self.by_api:  # can't detect async email sending in test
                mails_for_record = [
                    m for m in mailbox if actual_metadata["metadata"]["title"] in m.html
                ]
                assert len(mails_for_record) == len(expected_owners)
                for owner in expected_owners:
                    owner_sent_mail = [
                        m for m in mails_for_record if m.recipients == [owner["email"]]
                    ][0]
                    self.app.logger.debug(
                        f"owner to send mail: {pformat(owner['email'])}"
                    )
                    self.app.logger.debug(
                        f"owner_sent_mail recipients: "
                        f"{pformat(owner_sent_mail.recipients)}"
                    )
                    assert (
                        owner_sent_mail.subject
                        == "Your NEH Open Access Deposit is Ready"
                    )
                    assert owner_sent_mail.recipients == [owner["email"]]
                    assert (
                        owner_sent_mail.sender == self.app.config["MAIL_DEFAULT_SENDER"]
                    )
                    assert (
                        "Your NEH Open Access Deposit is Ready" in owner_sent_mail.html
                    )
                    assert (
                        "Your NEH Open Access Deposit is Ready" in owner_sent_mail.body
                    )
                    assert actual_metadata["metadata"]["title"] in owner_sent_mail.html
                    assert actual_metadata["metadata"]["title"] in owner_sent_mail.body
                    assert (
                        actual_metadata["pids"]["doi"]["identifier"]
                        in owner_sent_mail.html
                    )
                    assert (
                        actual_metadata["pids"]["doi"]["identifier"]
                        in owner_sent_mail.body
                    )
                    assert actual_metadata["links"]["self_html"] in owner_sent_mail.html
                    assert actual_metadata["links"]["self_html"] in owner_sent_mail.body
                    # Get the actual community URL from the community record
                    # (it may be /communities/ or /collections/ depending on context)
                    community_record = current_communities.service.read(
                        system_identity, id_=community_id
                    )
                    community_url = community_record.links["self_html"]
                    assert community_url in owner_sent_mail.html
                    assert community_url in owner_sent_mail.body

        else:
            assert actual_metadata["parent"]["access"]["owned_by"] == {
                "user": str(uploader_id)
            }
            assert actual_metadata["parent"]["access"]["grants"] == []

    def _check_successful_import(
        self,
        actual: dict,
        record_files: list,
        expected: TestRecordMetadataWithFiles,
        community: dict,
        uploader_id: str,
        mailbox,
        mocker,
    ):
        assert self.app
        actual_metadata = actual.get("metadata")
        assert actual_metadata

        actual_import_id = actual.get("source_id")
        assert actual_import_id == next(
            i.get("identifier")
            for i in actual_metadata.get("metadata", {}).get("identifiers")
            if i.get("scheme") == "import-recid"
        )

        actual_record_id = actual_metadata.get("id")
        assert actual_record_id == actual.get("record_id")

        actual_record_url = actual.get("record_url")
        assert actual_record_url == (
            f"{self.app.config['SITE_UI_URL']}/records/{actual_record_id}"
        )

        actual_collection_id = actual.get("collection_id")
        assert actual_collection_id in [community["id"], community["slug"]]
        assert actual_collection_id == actual_metadata.get("parent", {}).get(
            "communities", {}
        ).get("entries", [])[0].get("id")

        assert actual.get("errors") == []

        self._check_response_files(actual.get("files"), record_files)

        # add ids and checksums from actual file entries to the expected
        # file entries to compare file entries in metadata
        for k, f in expected.file_entries.items():
            f["id"] = actual_metadata["files"]["entries"][k]["id"]
            f["checksum"] = actual_metadata["files"]["entries"][k]["checksum"]
        assert expected.compare_published(actual_metadata)

        self._check_owners(
            actual_metadata,
            expected,
            uploader_id,
            community["id"],
            community["slug"],
            mailbox,
            mocker,
        )

        # Check the record in the database
        record_id1 = actual_metadata.get("id")
        rdm_record = records_service.read(system_identity, id_=record_id1).to_dict()
        assert expected.compare_published(rdm_record)

    def _check_failed_import(
        self, import_result: dict, expected_error_list: list[dict]
    ):
        assert import_result["status"] == "error"
        assert import_result.get("errors") == expected_error_list

    def check_result_data(
        self,
        import_results: dict,
        files: list,
        metadata_sources: list,
        community: dict,
        uploader_id: str,
        mailbox,
        mocker,
    ) -> None:
        """Check the data of the import results."""
        assert self.app
        expected_error_count = len([e for e in self.expected_errors if e])
        if expected_error_count > 0:
            assert len(import_results["data"]) == 0
            return
        assert (
            len(import_results["data"]) == len(metadata_sources) - expected_error_count
        )
        files_per_item = len(files) // len(metadata_sources)
        for idx, actual_record_result in enumerate(import_results["data"]):
            expected_error_list = self.expected_errors[idx]
            assert actual_record_result["item_index"] == idx

            if expected_error_list:
                self._check_failed_import(actual_record_result, expected_error_list)
            else:
                record_files = files[
                    idx * files_per_item : (idx + 1)
                    * files_per_item  # noqa: E203
                ]

                self._check_successful_import(
                    actual_record_result,
                    record_files,
                    metadata_sources[idx],
                    community,
                    uploader_id,
                    mailbox,
                    mocker,
                )

    def _do_api_import(
        self,
        community: dict,
        file_streams: list,
        token: str,
        metadata_source_objects: list[TestRecordMetadataWithFiles],
    ) -> tuple[dict | None, int]:
        assert self.app
        with self.app.test_client() as client:
            actual_response = client.post(
                f"{self.app.config['SITE_API_URL']}/import/{community['slug']}",
                content_type="multipart/form-data",
                data={
                    "metadata": json.dumps([
                        copy.deepcopy(m.metadata_in) for m in metadata_source_objects
                    ]),
                    "id_scheme": "import-recid",
                    "review_required": "true",
                    "strict_validation": "true",
                    "all_or_none": "true",
                    "notify_record_owners": "false",  # Will error otherwise
                    "files": file_streams,
                },
                headers={
                    "Content-Type": "multipart/form-data",
                    "Authorization": f"Bearer {token}",
                },
            )

        return actual_response.json, actual_response.status_code

    def test_import_records_service_load(
        self,
        running_app,
        db,
        minimal_community_factory,
        user_factory,
        search_clear,
        mock_send_remote_api_update_fixture,
        mailbox,
        mocker,
        monkeypatch,
        nested_unit_of_work,
        test_sample_files_folder,
    ):
        """Test importing a record via the service."""
        self.app = running_app.app
        self.test_sample_files_folder = test_sample_files_folder
        u = user_factory(email="test@example.com", token=True, oauth_id=None)
        user_id = u.user.id
        identity = get_identity(u.user)
        identity.provides.add(authenticated_user)

        community_record = minimal_community_factory(
            slug="neh",
            owner=u.user.id,
            access=self.community_access_override,
        )
        community = community_record.to_dict()

        submitter_identity, submitter_token = self.make_submitter(
            user_factory, community["id"]
        )
        if not submitter_identity:
            submitter_identity, submitter_token = identity, u.allowed_token

        # make_submitter may return a user id (API permission cases) or an
        # Identity (service path). Prefer the actual importer for ownership
        # assertions when a distinct submitter was configured.
        if isinstance(submitter_identity, int):
            importer_user_id = submitter_identity
        elif submitter_identity is not identity:
            importer_user_id = submitter_identity.id
        else:
            importer_user_id = user_id

        if not self.by_api:
            login_user(submitter_identity.user)  # type: ignore

        # Remember to close the file streams after the import is complete
        files, file_list, file_streams = self.files_to_upload
        files_per_item = len(file_list) // len(self.metadata_sources)

        metadata_source_objects = []
        for idx, metadata_source in enumerate(self.metadata_sources):
            item_files = file_list[
                idx * files_per_item : (idx + 1)
                * files_per_item  # noqa: E203
            ]
            file_entries = {f["key"]: f for f in item_files}
            test_metadata = TestRecordMetadataWithFiles(
                metadata_in=metadata_source,
                community_list=[community],
                owner_id=importer_user_id,
                file_entries=file_entries,
            )

            test_metadata.update_metadata({
                "metadata|identifiers": [
                    {
                        "identifier": f"1234567890{str(idx)}",
                        "scheme": "import-recid",
                    }
                ]
            })
            metadata_source_objects.append(test_metadata)

        if self.by_api and submitter_token:
            monkeypatch.setattr(
                "invenio_records_resources.services.uow.UnitOfWork", nested_unit_of_work
            )
            import_results, status_code = self._do_api_import(
                community,
                file_streams,
                submitter_token,
                metadata_source_objects,
            )

        else:
            load_community_needs(identity)
            service = current_record_importer_service
            import_results = service.import_records(
                identity=submitter_identity,
                file_data=files,
                metadata=[
                    copy.deepcopy(m.metadata_in) for m in metadata_source_objects
                ],
                community_id=community["id"],
            )
            status_code = None

        for file in file_streams:
            file.close()

        assert import_results is not None
        if self.check_result_status(import_results, status_code):
            self.check_result_errors(import_results)
            self.check_result_data(
                import_results,
                files,
                metadata_source_objects,
                community,
                importer_user_id,
                mailbox,
                mocker,
            )


# class TestImportServiceChapter(BaseImportRecordsServiceLoadTest):
#     @property
#     def metadata_source(self):
#         return sample_metadata_chapter_pdf


# class TestImportServiceChapter2(BaseImportRecordsServiceLoadTest):
#     @property
#     def metadata_source(self):
#         return sample_metadata_chapter2_pdf


class TestServiceJArticle(ServiceTest):
    """Test importing two journal articles with no errors."""

    @property
    def metadata_sources(self):  # noqa: D102
        return [
            copy.deepcopy(sample_metadata_journal_article_pdf),
            copy.deepcopy(sample_metadata_journal_article2_pdf),
        ]
