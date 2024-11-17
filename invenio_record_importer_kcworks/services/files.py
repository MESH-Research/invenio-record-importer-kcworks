#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2024 Mesh Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

from xml.dom.minidom import Element
from flask import current_app as app
import fnmatch
from invenio_access.permissions import system_identity
from invenio_files_rest.errors import InvalidKeyError, BucketLockedError
from invenio_pidstore.errors import PIDUnregistered
from invenio_rdm_records.proxies import (
    current_rdm_records_service as records_service,
)
from invenio_record_importer_kcworks.errors import (
    FileUploadError,
    UploadFileNotFoundError,
)
from invenio_record_importer_kcworks.utils.utils import (
    normalize_string,
    valid_date,
)

# from invenio_records_resources.services.errors import (
#     FileKeyNotFoundError,
# )
from invenio_records_resources.services.uow import (
    unit_of_work,
    UnitOfWork,
    RecordCommitOp,
)
import os
from pprint import pformat
from pathlib import Path
import re
from sqlalchemy.orm.exc import NoResultFound
from typing import Optional
import unicodedata
from urllib.parse import unquote


class FilesHelper:
    def __init__(self, is_draft: bool):
        self.files_service = (
            records_service.draft_files if is_draft else records_service.files
        )

    @staticmethod
    def sanitize_filenames(directory) -> list:
        changed = []
        for path, dirs, files in os.walk(directory):
            for filename in fnmatch.filter(files, "*[“”‘’]*"):
                file_path = os.path.join(path, filename)
                newname = re.sub(r"[“”‘’]", "", filename)
                new_file_path = os.path.join(path, newname)
                if file_path != new_file_path:
                    os.rename(file_path, new_file_path)
                    changed.append(new_file_path)
        return changed

    @unit_of_work()
    def set_to_metadata_only(
        self, draft_id: str, uow: Optional[UnitOfWork] = None
    ):
        try:
            record = records_service.read(system_identity, draft_id)._record
            if record.files.entries:
                for k in record.files.entries.keys():
                    self._delete_file(draft_id, k, records_service.files)
        except PIDUnregistered:
            pass

        try:
            record = records_service.read_draft(
                system_identity, draft_id
            )._record
            if record.files.entries:
                for k in record.files.entries.keys():
                    self._delete_file(draft_id, k, records_service.draft_files)
        except NoResultFound:
            pass
        record.files.enabled = False
        record["access"]["status"] = "metadata-only"
        uow.register(RecordCommitOp(record))

    @unit_of_work()
    def _clear_managers(
        self,
        draft_id: str,
        record,
        is_draft: bool,
        is_published: bool,
        uow: Optional[UnitOfWork] = None,
    ):

        def inner_clear_manager(record, is_published):
            if is_published:
                record.files.unlock()
            record.files.delete_all(
                remove_obj=True,
                softdelete_obj=False,
                remove_rf=True,
            )

        # Either way, we need to ensure the appropriate record
        # file managers are empty
        if is_draft:
            app.logger.info("Handling record with existing draft")
            draft_record = records_service.draft_files._get_record(
                draft_id, system_identity, "delete_files"
            )
            if draft_record.files.entries:
                inner_clear_manager(draft_record, is_published)
            app.logger.info("    deleted all files from existing draft")
        if is_published:
            app.logger.info("Handling published record")
            record = records_service.files._get_record(
                draft_id, system_identity, "delete_files"
            )
            if record.files.entries:
                inner_clear_manager(record, is_published)
            app.logger.info(
                "    deleted all files from existing " "published record"
            )
        else:
            app.logger.info(
                "    no files attached to existing record "
                "and no new files to be uploaded"
            )

    @unit_of_work()
    def _delete_file(
        self,
        draft_id: str,
        key: str,
        files_service=None,
        files_type: str = "",
        uow: Optional[UnitOfWork] = None,
    ) -> bool:
        if files_service is None:
            files_service = self.files_service
        read_method = (
            records_service.read
            if files_service == records_service.files
            else records_service.read_draft
        )

        def inner_delete_file(key: str):
            # try:
            files_service.delete_file(system_identity, draft_id, key)
            # except NoResultFound:
            #     try:
            #         records_service.files.delete_file(
            #             system_identity, draft_id, key
            #         )
            #     except FileKeyNotFoundError as e:
            #         app.logger.info("file not found for deletion")
            #         print("file not found for deletion")
            #         raise e

            # try:
            record = read_method(system_identity, draft_id)._record
            assert key not in record.files.entries.keys()
            # except PIDUnregistered:
            #     record = records_service.read_draft(
            #         system_identity, draft_id
            #     )._record
            #     assert key not in record.files.entries.keys()

        try:
            inner_delete_file(key)
        except BucketLockedError:
            try:
                record = read_method(system_identity, draft_id)._record
                print("attempting to unlock files:", record.files.entries)
                record.files.unlock()
                # Duplicating logic from files_service.delete_file
                # to allow unlocking the published record files
                removed_file = record.files.delete(
                    key, softdelete_obj=False, remove_rf=True
                )
                files_service.run_components(
                    "delete_file",
                    system_identity,
                    draft_id,
                    key,
                    record,
                    removed_file,
                    uow=uow,
                )
                uow.register(RecordCommitOp(record))
                assert key not in record.files.entries.keys()

                app.logger.debug(
                    "...file key existed on record but was empty and was "
                    "removed. This probably indicates a prior failed upload."
                )
                app.logger.debug(pformat(removed_file))
                return True
            except Exception as e:
                app.logger.error(
                    f"    failed to unlock files for record {draft_id}..."
                )
                raise e

        record = read_method(system_identity, draft_id)._record
        assert key not in record.files.entries.keys()

        return True

    @unit_of_work()
    def handle_record_files(
        self,
        metadata: dict,
        file_data: dict,
        existing_record: Optional[dict] = {},
        source_filepaths: Optional[dict] = {},
        uow: Optional[UnitOfWork] = None,
    ) -> dict:
        """
        Ensure that the files for a record are uploaded correctly.

        If the record already exists, we need to check if the files have
        changed. If they have, we need to upload the new files and delete the
        old files. If they have not changed, we can skip the upload.

        If the record has been published and we are not working with a draft
        of a new version, we need to unlock the published record files before
        we can upload new files. We then lock the files after the upload is
        complete.

        Note that file data is managed indirectly by a file manager object
        that is separate from the record metadata. The file manager is what
        actually knows about the files for a record. The file manager is
        synchronized with the record metadata, but it is the source of truth
        for the file data. In a straightforward upload for a new draft, the
        record service's file service handles operations on the record's file
        manager. But when a previous draft or published record is involved,
        we need to inspect and possibly alter the file manager's state
        directly.

        This is complicated by the fact that the record service actually has
        two separate file services, one for drafts of a record and one for
        the published version of the record (the drafts and published version
        share the same id). We need to ensure both that the draft file service
        has updated the file manager for the draft and that (if a published
        version exists) the published record file service has updated the
        manager for any existing published version. Most of this checking is
        done in the `_compare_existing_files` method.

        params:
            metadata (dict): the complete record metadata for the new
                draft before the files are uploaded or altered. The `files`
                value will either be a dict with an empty `entries` dict
                or will contain the file information for the previous draft
                or published record if there was one. It cannot be used as
                a source of file data for the new draft.
            file_data (dict): the file data to be uploaded to the new draft.
                This will be drawn directly from the import file data and is
                the source of truth for the new draft. It is a dictionary
                shaped like the record's `files` property.
            existing_record (dict): the existing record metadata, if we are
                updating a draft of a published record or an unpublished
                preexisting draft. It will only be empty if we are creating a
                new record with no preexisting drafts.
            source_filepaths (dict): a dictionary mapping file keys to the
                source file paths for the files to be uploaded. FIXME: We
                need to implement an input pathway to provide this value
                for all imports.
            uow (UnitOfWork): the unit of work to register the commit operation
                for the record if we are locking and unlocking a record
                (provided by the unit of work decorator if
                not provided by the caller)

        NOTE: The files in `metadata` and `existing_record` will often be
        different. `existing_record` may have files that were already on
        any pre-existing published or draft record. This is the
        case *even if* we tried to update the file data during creation of
        the new draft we're editing, because the new draft creation process
        copies the files from the existing record over to the new draft.
        If we are creating a brand new record, with no prior drafts, `metadata`
        will have an empty files property. If there is an existing draft,
        `metadata` will have the same files as `existing_record`. This means
        that only `file_data` provides the file data for the current import.

        Returns:
            dict: A dictionary with the new draft's files after any necessary
                file uploads and deletions have been completed. The dictionary
                is shaped like the record's `files` property (after it is
                dumped to a dictionary in a record result object).

        """
        print(f"handle_record_files metadata: {pformat(metadata)}")
        print(f"handle_record_files file_data: {pformat(file_data)}")
        print(
            f"handle_record_files existing_record.files: "
            f"{pformat(existing_record.get('files') if existing_record else None)}"  # noqa: E501
        )
        assert metadata["files"]["enabled"] is True
        files_to_upload = {k: v for k, v in file_data["entries"].items()}
        uploaded_files = {}
        same_files = False

        if existing_record:
            same_files, files_to_upload = self._compare_existing_files(
                metadata["id"],
                existing_record["is_draft"],
                existing_record["is_published"],
                existing_record["files"]["entries"],
                file_data["entries"],
            )

        if same_files:
            app.logger.info(
                "    skipping uploading files (same already uploaded)..."
            )
            uploaded_files = existing_record["files"]
        elif len(files_to_upload) > 0:
            app.logger.info("    uploading new files...")
            app.logger.warning("file data: %s", pformat(files_to_upload))
            # FIXME: Below is an implementation detail for the CORE
            # migration that should be removed when we use this method
            # for all imports.
            if not source_filepaths:
                first_file = next(iter(files_to_upload))
                source_filepaths = {
                    first_file: metadata["custom_fields"][
                        "hclegacy:file_location"
                    ]
                }

            # If we're updating a draft of a published record, we need to
            # unlock the published record files before we can upload new
            # files.
            need_to_unlock = (
                existing_record.get("is_published")
                if existing_record
                else False
            )
            record = None

            if need_to_unlock:
                app.logger.warning("    unlocking published record files...")
                record = records_service.read(
                    system_identity, existing_record["id"]
                )._record
                record.files.unlock()
                uow.register(RecordCommitOp(record))

            uploaded_files = self._upload_draft_files(
                metadata["id"],
                files_to_upload,
                source_filepaths,
            )

            if need_to_unlock:
                record.files.lock()
                uow.register(RecordCommitOp(record))
        else:
            app.logger.info(
                "    no files to upload marking as " "metadata-only..."
            )
            self.set_to_metadata_only(metadata["id"])
        print("returning uploaded_files:", pformat(uploaded_files))

        return uploaded_files

    # FIXME: This method is not currently used.
    @unit_of_work()
    def _retry_file_initialization(
        self,
        draft_id: str,
        k: str,
        uow: Optional[UnitOfWork] = None,
    ) -> bool:
        existing_record = self.files_service._get_record(
            draft_id, system_identity, "create_files"
        )
        print("retrying initialization for file:", k)
        print("existing record file:", existing_record.files.entries[k])

        if not existing_record.files.entries[k].metadata:
            existing_record.files.unlock()

            # Duplicating logic from files_service.delete_file
            # to allow unlocking the published record files
            removed_file = existing_record.files.delete(
                k, softdelete_obj=False, remove_rf=True
            )
            self.files_service.run_components(
                "delete_file",
                system_identity,
                draft_id,
                k,
                existing_record,
                removed_file,
                uow=uow,
            )
            uow.register(RecordCommitOp(existing_record))
            assert k not in existing_record.files.entries.keys()

            app.logger.debug(
                "...file key existed on record but was empty and was "
                "removed. This probably indicates a prior failed upload."
            )
            app.logger.debug(pformat(removed_file))

            return True
        else:
            app.logger.error(existing_record.files.entries)
            app.logger.error(
                "    file key already exists on record but is not found in "
                "draft metadata retrieved by record service"
            )
            raise InvalidKeyError(
                f"File key {k} already exists on record but is not found in "
                "draft metadata retrieved by record service"
            )

    def _sanitize_filename(self, filename: str) -> str:

        long_filename = filename.replace(
            "/srv/www/commons/current/web/app/uploads/humcore/", ""
        )
        long_filename = long_filename.replace(
            "/srv/www/commons/shared/uploads/humcore/", ""
        )
        long_filename = long_filename.replace(
            "/app/site/web/app/uploads/humcore/", ""
        )
        # app.logger.debug(filename)
        # app.logger.debug(source_filename)
        # app.logger.debug(normalize_string(filename))
        # app.logger.debug(normalize_string(unquote(source_filename)))
        try:
            assert normalize_string(filename) in normalize_string(
                unquote(filename)
            )
        except AssertionError:
            app.logger.error(
                f"    file key {filename} does not match source filename"
                f" {filename}..."
            )
            raise UploadFileNotFoundError(
                f"File key from metadata {filename} not found in source "
                f"file path {filename}"
            )
        return long_filename

    def _find_file_path(self, filename: str, key: str) -> Path:

        file_path = (
            Path(app.config["RECORD_IMPORTER_FILES_LOCATION"]) / filename
        )
        try:
            assert file_path.is_file()
        except AssertionError:
            try:
                full_length = len(filename.split("."))
                try_index = -2
                while abs(try_index) + 2 <= full_length:
                    file_path = Path(
                        app.config["RECORD_IMPORTER_FILES_LOCATION"],
                        ".".join(filename.split(".")[:try_index]) + "." + key,
                    )
                    if file_path.is_file():
                        break
                    else:
                        try_index -= 1
                assert file_path.is_file()
            except AssertionError:
                try:
                    file_path = Path(
                        unicodedata.normalize("NFD", str(file_path))
                    )
                    assert file_path.is_file()
                except AssertionError:
                    raise UploadFileNotFoundError(
                        f"    file not found for upload {file_path}..."
                    )
        return file_path

    def _upload_draft_files(
        self,
        draft_id: str,
        files_dict: dict[str, dict],
        source_filenames: dict[str, str],
    ) -> dict:
        output = {}

        app.logger.debug(
            f"files_dict in _upload_draft_files: {pformat(files_dict.keys())}"
        )

        for k, v in files_dict.items():
            long_filename = self._sanitize_filename(source_filenames[k])

            file_path = self._find_file_path(long_filename, k)
            app.logger.debug(f"    uploading file: {file_path}")

            try:
                initialization = self.files_service.init_files(
                    system_identity, draft_id, data=[{"key": k}]
                ).to_dict()
                assert (
                    len(
                        [
                            e["key"]
                            for e in initialization["entries"]
                            if e["key"] == k
                        ]
                    )
                    == 1
                )
                app.logger.debug(f"initialization: {pformat(initialization)}")
            except InvalidKeyError as e:
                app.logger.error(
                    f"    failed to initialize file upload for {draft_id}..."
                )
                raise e
            except Exception as e:
                app.logger.error(
                    f"    failed to initialize file upload for {draft_id}..."
                )
                raise e

            try:
                with open(
                    file_path,
                    "rb",
                ) as binary_file_data:
                    binary_file_data.seek(0)
                    self.files_service.set_file_content(
                        system_identity, draft_id, k, binary_file_data
                    )

            except Exception as e:
                app.logger.error(
                    f"    failed to upload file content for {draft_id}..."
                )
                raise e

            try:
                self.files_service.commit_file(system_identity, draft_id, k)
            except Exception as e:
                app.logger.error(
                    f"    failed to commit file upload for {draft_id}..."
                )
                raise e

            output[k] = "uploaded"

        result_record = self.files_service.list_files(
            system_identity, draft_id
        ).to_dict()
        try:
            assert all(
                r["key"]
                for r in result_record["entries"]
                if r["key"] in files_dict.keys()
            )
            for v in result_record["entries"]:
                assert v["key"] == k
                assert v["status"] == "completed"
                app.logger.debug(f"size: {v['size']}  {files_dict[k]['size']}")
                if str(v["size"]) != str(files_dict[k]["size"]):
                    raise FileUploadError(
                        f"Uploaded file size ({v['size']}) does not match "
                        f"expected size ({files_dict[k]['size']})"
                    )
                assert valid_date(v["created"])
                assert valid_date(v["updated"])
                assert not v["metadata"]
        except AssertionError:
            app.logger.error(
                "    failed to properly upload file content for"
                f" draft {draft_id}..."
            )
            app.logger.error(f"result is {pformat(result_record['entries'])}")

        # Handle published records without drafts, where record needs
        # file metadata from draft files service (usually synced during
        # draft publication but we're not publishing a draft here)
        try:
            check_record = records_service.read(system_identity, id_=draft_id)
            if check_record.to_dict()["files"]["entries"] == {}:
                check_draft = records_service.read_draft(
                    system_identity, id_=draft_id
                )
                check_record._record.files.sync(check_draft._record.files)
                check_record._record.files.lock()
        except PIDUnregistered:
            pass

        return output

    def _compare_existing_files(
        self,
        draft_id: str,
        is_draft: bool,
        is_published: bool,
        old_files: dict[str, dict],
        new_entries: dict[str, dict],
    ) -> bool:
        """
        Compare existing files to new entries.

        Note that this function should only be called when we are updating a
        pre-existing record:
        - a published record without a draft
        - a pre-existing draft of a published record
        - a pre-existing draft of a never-published record

        draft_id (str): id of the draft record we're checking. (Although this
            will always be a draft record, it may be a draft revising a
            previously published record.)
        is_draft (bool): True if working with a draft record whether a draft
            of a published record or a draft that has never been published.
        is_published (bool): True if checking a record that has been published,
            whether or not a draft of that published record already exists.
        old_files (dict): existing files on the previously saved record
            metadata. This could be a prior published record if no draft yet
            exists, or an existing draft of that published record. (Dict is
            shaped like the "entries" key of a record's files property.)
        new_entries (dict): new files to be uploaded to the record. (Dict is
            shaped like the "entries" key of a record's files property.)

        If files are different, delete any wrong files from the record. If
        files are missing from the file services and/or the draft metadata,
        ensure that the files are deleted from the draft record's file manager.
        If this is a published record with no draft, ensure that the files are
        deleted from the published record's file manager.

        Note that there are separate files services for operations on the
        file managers for the published and draft versions of a record.
        We have to ensure that both file services (and the managers for both
        versions of the record) are updated correctly. Rather than trying to
        anticipate which service to use based on the state of the record, we
        simply try to check the record's file manager using both services and
        update both as needed to align with the new import file data.

        Note too that the "entries" property of a the return object from the
        files service is a list, not a dict. The "entries" property of the
        return object of the record metadata is a dict whose values are the
        same as the items in the files service's list.

        We also have to check for the cases where
        - the record lacks files in the update metadata. In this case simply
          return False. (FIXME: What if only some files are missing? Will
          we generate a key error when we try to initialize?)
        - the record has files that are not present in the update metadata.
          In this case we want to ensure that the extra files are deleted from
          the draft record's file manager. We still return False.  FIXME: Do
          we need to return False here? We don't have to upload anything.
        - the record has no files manager (record without files). In this case
          just return False.
        - the previous version of the draft record had files, but the new
          version does not. In this case we want to ensure that the files are
          deleted from the draft record's file manager.
        - the prior existing draft record has different files from the
          published record (draft was not published). In this case we want
          to ensure that the extra files from the draft are removed from the
          draft file manager, even if they are not present in the published
          record file manager.

        Returns a tuple with two elements:
            bool: True if files are the same, False otherwise.
            list: A list of keys for files that are to be uploaded.
        """
        print("is_draft:", is_draft)
        same_files = True
        files_to_upload = {}

        def inner_compare_files(
            existing_files: list,
            files_service,
            inner_files_to_upload: dict,
            inner_same_files: bool,
        ):
            normalized_new_keys = [
                unicodedata.normalize("NFC", k) for k in new_entries.keys()
            ]
            print("normalized new keys:", normalized_new_keys)
            old_wrong_files = [
                f
                for f in existing_files
                if unicodedata.normalize("NFC", f["key"])
                not in normalized_new_keys
            ]
            print("old_wrong_files:", old_wrong_files)
            for o in old_wrong_files:
                print("old wrong file:", o)
                self._delete_file(
                    draft_id,
                    o["key"],
                    files_service=files_service,
                )
                app.logger.info("    deleted wrong file: %s", o["key"])

            for k, v in new_entries.items():
                wrong_file = False
                existing_file = [
                    f
                    for f in existing_files
                    if unicodedata.normalize("NFC", f["key"])
                    == unicodedata.normalize("NFC", k)
                ]

                if len(existing_file) == 0:
                    inner_same_files = False
                    inner_files_to_upload[k] = new_entries[k]
                    print("no existing file found")

                elif (existing_file[0]["status"] == "pending") or (
                    str(v["size"]) != str(existing_file[0]["size"])
                ):
                    inner_same_files = False
                    inner_files_to_upload[k] = new_entries[k]
                    wrong_file = True
                    print("pending or size mismatch")

                if wrong_file:
                    self._delete_file(
                        draft_id,
                        existing_file[0]["key"],
                        files_service=files_service,
                    )
            return inner_same_files, inner_files_to_upload

        existing_published_files = []
        try:
            published_files_request = records_service.files.list_files(
                system_identity, draft_id
            ).to_dict()
            print("published files request:", published_files_request)
            existing_published_files = published_files_request.get(
                "entries", []
            )
        except NoResultFound:  # draft record
            pass
        except AttributeError:  # published record without files manager
            pass

        existing_draft_files = []
        try:
            draft_files_request = records_service.draft_files.list_files(
                system_identity, draft_id
            ).to_dict()
            print("draft files request:", draft_files_request)
            existing_draft_files = draft_files_request.get("entries", [])
        except NoResultFound:
            pass

        if (is_draft and is_published) or (is_draft and not is_published):
            existing_files = existing_draft_files
            files_service = records_service.draft_files
        elif is_published and not is_draft:
            existing_files = existing_published_files
            files_service = records_service.files
        else:
            raise ValueError("Invalid record state for file comparison")

        print("files service:", files_service.config)
        print("existing files:", existing_files)
        print("new entries:", new_entries)

        # If the existing record has no files
        if len(existing_files) == 0 or old_files == {}:
            # Ensure that the appropriate file managers are empty
            self._clear_managers(
                draft_id, existing_files, is_draft, is_published
            )

            # If there are new files to be uploaded, set same_files to False
            if len(new_entries) > 0:
                same_files = False
                print(
                    "    new files to be uploaded that are not "
                    "present in the existing record"
                )

            print("neither existing record nor upload data has files")

        # If the existing record has files
        # includes cases where
        # - the upload data has files not present in the existing record
        # - the upload data has files that are present in the existing record
        # - the existing record has files not present in the upload data
        # - the existing draft record has files that differ from the published
        #   record (and from the upload data)
        if len(existing_files) > 0:
            same_files, files_to_upload = inner_compare_files(
                existing_files, files_service, files_to_upload, same_files
            )

        # If the existing record is a draft of a published record, we need to
        # compare the draft files to the published files.
        if is_draft and is_published:
            pub_same_files, pub_files_to_upload = inner_compare_files(
                existing_published_files,
                records_service.files,
                files_to_upload,
                same_files,
            )
            if pub_same_files != same_files:
                print("draft and published same_files results are different")
                print("draft same_files:", same_files)
                print("published same_files:", pub_same_files)
                files_to_upload = pub_files_to_upload
            if list(files_to_upload.keys()) != list(
                pub_files_to_upload.keys()
            ):
                print("draft and published files to upload are different")
                print("draft files to upload:", files_to_upload)
                print("published files to upload:", pub_files_to_upload)

        print("returning same files:", same_files)
        print("returning files to upload:", files_to_upload)
        return same_files, files_to_upload
