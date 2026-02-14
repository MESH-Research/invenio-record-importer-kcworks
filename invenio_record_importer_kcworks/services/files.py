#! /usr/bin/env python
#
# Copyright (C) 2023-2024 Mesh Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

import os
import re
import unicodedata
from io import BufferedReader
from pathlib import Path
from pprint import pformat
from tempfile import SpooledTemporaryFile
from urllib.parse import unquote

from flask import current_app as app
from invenio_access.permissions import system_identity
from invenio_drafts_resources.resources.records.errors import (
    DraftNotCreatedError,
)
from invenio_files_rest.errors import BucketLockedError, InvalidKeyError
from invenio_pidstore.errors import PIDDoesNotExistError, PIDUnregistered
from invenio_rdm_records.proxies import current_rdm_records_service as records_service
from invenio_records_resources.services.errors import (
    FileKeyNotFoundError,
)
from invenio_records_resources.services.uow import (
    RecordCommitOp,
    UnitOfWork,
    unit_of_work,
)
from sqlalchemy.orm.exc import NoResultFound

from invenio_record_importer_kcworks.errors import (
    FileUploadError,
    UploadFileNotFoundError,
)
from invenio_record_importer_kcworks.types import FileData, FileUploadResult
from invenio_record_importer_kcworks.utils.utils import (
    normalize_string,
    valid_date,
)


class FilesHelper:
    """Helper functions for dealing with uploaded files."""

    def __init__(self, is_draft: bool):
        """Initialize a FilesHelper instance."""
        self.files_service = (
            records_service.draft_files if is_draft else records_service.files
        )

    @staticmethod
    def sanitize_filenames(directory) -> list:
        """Sanitize all filenames in a directory.

        Uses sanitize_filename() to remove dangerous characters and
        problematic Unicode punctuation from filenames, then renames
        the files if their names changed.

        Args:
            directory: The directory path to walk through and sanitize filenames.

        Returns:
            list: A list of file paths with the filenames sanitized.
        """
        changed = []
        for path, dirs, files in os.walk(directory):
            for filename in files:
                file_path = os.path.join(path, filename)
                sanitized_name = FilesHelper.sanitize_filename(filename)
                if filename != sanitized_name:
                    new_file_path = os.path.join(path, sanitized_name)
                    os.rename(file_path, new_file_path)
                    changed.append(new_file_path)
        return changed

    def sanitize_all_filenames(
        self, files_to_upload: dict[str, dict], files: list[FileData]
    ) -> dict[str, dict]:
        """Sanitize filenames in metadata and file objects."""
        sanitized_files_to_upload = {}
        filename_mapping = {}  # old_key -> sanitized_key

        for old_key, file_info in files_to_upload.items():
            sanitized_key = FilesHelper.sanitize_filename(old_key)
            filename_mapping[old_key] = sanitized_key
            sanitized_files_to_upload[sanitized_key] = file_info

        # Update FileData.filename values to match sanitized keys
        for file_data_obj in files:
            if file_data_obj.filename:
                filename_part = file_data_obj.filename.split("/")[-1]
                # Find the matching sanitized key
                sanitized_filename = filename_mapping.get(
                    filename_part, FilesHelper.sanitize_filename(filename_part)
                )
                # Update filename, preserving path if present
                if "/" in file_data_obj.filename:
                    path_part = "/".join(file_data_obj.filename.split("/")[:-1])
                    file_data_obj.filename = f"{path_part}/{sanitized_filename}"
                else:
                    file_data_obj.filename = sanitized_filename

        return sanitized_files_to_upload

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        r"""Sanitize filename.

        Removes dangerous and problematic characters that could:
        - Break HTTP header parsing (control chars, quotes, backslashes, newlines)
        - Be used in code injection attempts

        Preserves:
        - Non-Latin scripts (Arabic, Chinese, etc.) for proper UTF-8 encoding
        - Standard alphanumeric characters and safe punctuation

        Args:
            filename: The original filename to sanitize.

        Returns:
            A sanitized filename safe for use in Content-Disposition headers.

        Examples:
            >>> sanitize_filename('file"name.pdf')
            'filename.pdf'
            >>> sanitize_filename('file\nname.pdf')
            'filename.pdf'
        """
        # Remove control characters
        filename = re.sub(r"[\x00-\x1f\x7f]", "", filename)

        # Replace problematic characters that can confuse header
        # parsers or be used for injection
        problematic_punctuation = {
            "\n": "",  # Remove newlines
            "\r": "",  # Remove carriage returns
            '"': "",  # Remove straight double quotes (injection risk)
            "\\": "",  # Remove backslashes (injection risk)
            "/": "",  # Remove forward slashes (path traversal risk)
            "\u201c": '"',  # Left double quotation mark (U+201C)
            "\u201d": '"',  # Right double quotation mark (U+201D)
            "\u2018": "'",  # Left single quotation mark (U+2018)
            "\u2019": "'",  # Right single quotation mark (U+2019)
            "–": "-",  # En dash (U+2013)
            "—": "-",  # Em dash (U+2014)
            "…": "...",  # Horizontal ellipsis (U+2026)
            "«": '"',  # Left-pointing double angle quotation mark (U+00AB)
            "»": '"',  # Right-pointing double angle quotation mark (U+00AE)
            "‹": "'",  # Single left-pointing angle quotation mark (U+2039)
            "›": "'",  # Single right-pointing angle quotation mark (U+203A)
        }
        for old_char, new_char in problematic_punctuation.items():
            filename = filename.replace(old_char, new_char)

        # Remove other potentially dangerous characters
        filename = re.sub(r"[^\w\s.\-()\[\]{}@!#$%&*+=,;:?]", "", filename)

        # Clean up whitespace
        filename = re.sub(r"\s+", " ", filename)
        filename = filename.strip()

        # Collapse multiple consecutive dots
        # Handles file system traversal remnants
        filename = re.sub(r"\.{2,}", ".", filename)

        # Strip leading/trailing dots
        filename = filename.strip(".")

        return filename

    @unit_of_work()
    def set_to_metadata_only(self, draft_id: str, uow: UnitOfWork | None = None):
        if uow:
            try:
                record = records_service.read(system_identity, draft_id)._record
                if record.files.entries:
                    for k in record.files.entries.keys():
                        self._delete_file(draft_id, k, records_service.files)
            except PIDUnregistered:
                pass

            try:
                record = records_service.read_draft(system_identity, draft_id)._record
                if record.files.entries:
                    for k in record.files.entries.keys():
                        self._delete_file(draft_id, k, records_service.draft_files)
            except (NoResultFound, DraftNotCreatedError):
                pass
            record.files.enabled = False
            record["access"]["status"] = "metadata-only"
            uow.register(RecordCommitOp(record))
        else:
            raise RuntimeError("uow is required")

    @unit_of_work()
    def _clear_managers(
        self,
        draft_id: str,
        record,
        is_draft: bool,
        is_published: bool,
        uow: UnitOfWork | None = None,
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
            app.logger.info("deleted all files from existing draft")
        if is_published:
            app.logger.info("Handling published record")
            record = records_service.files._get_record(
                draft_id, system_identity, "delete_files"
            )
            if record.files.entries:
                inner_clear_manager(record, is_published)
            app.logger.info("deleted all files from existing published record")
        else:
            app.logger.info(
                "no files attached to existing record and no new files to be uploaded"
            )

    @unit_of_work()
    def _delete_file(
        self,
        draft_id: str,
        key: str,
        files_service=None,
        files_type: str = "",
        uow: UnitOfWork | None = None,
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
                assert uow
                uow.register(RecordCommitOp(record))
                assert key not in record.files.entries.keys()

                app.logger.debug(
                    "...file key existed on record but was empty and was "
                    "removed. This probably indicates a prior failed upload."
                )
                return True
            except Exception as e:
                app.logger.error(f"failed to unlock files for record {draft_id}...")
                raise e

        record = read_method(system_identity, draft_id)._record
        assert key not in record.files.entries.keys()

        return True

    @unit_of_work()
    def _unlock_files(self, existing_record, uow: UnitOfWork | None = None):
        need_to_unlock = (
            existing_record.get("is_published") if existing_record else False
        )
        record = None

        if need_to_unlock:
            app.logger.warning("unlocking published record files...")
            record = records_service.read(
                system_identity, existing_record["id"]
            )._record
            record.files.unlock()
            if uow:
                uow.register(RecordCommitOp(record))
            else:
                raise RuntimeError("uow is required")

            try:
                app.logger.warning("unlocking draft record files...")
                draft_record = records_service.read_draft(
                    system_identity, existing_record["id"]
                )._record
                draft_record.files.unlock()
                uow.register(RecordCommitOp(draft_record))
            except (NoResultFound, DraftNotCreatedError):
                pass

    @unit_of_work()
    def _lock_files(self, existing_record, uow: UnitOfWork | None = None):
        need_to_lock = existing_record.get("is_published") if existing_record else False
        record = None

        if need_to_lock:
            record = records_service.read(
                system_identity, id_=existing_record["id"]
            )._record
            record.files.lock()
            if uow:
                uow.register(RecordCommitOp(record))
            else:
                raise RuntimeError("uow is required")

            try:
                app.logger.warning("locking draft record files...")
                draft_record = records_service.read_draft(
                    system_identity, existing_record["id"]
                )._record
                draft_record.files.lock()
                uow.register(RecordCommitOp(draft_record))
            except (NoResultFound, DraftNotCreatedError):
                pass

    @unit_of_work()
    def handle_record_files(
        self,
        metadata: dict,
        file_data: dict | list[dict],
        files: list[FileData] = [],
        existing_record: dict | None = {},
        source_filepaths: dict | None = {},
        clean_filenames: bool = True,
        uow: UnitOfWork | None = None,
    ) -> dict[str, FileUploadResult]:
        r"""Ensure that the files for a record are uploaded correctly.

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
        we need to inspect and possibly alter the file manager\'s state
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
            file_data (dict): the metadata on files to be uploaded to the new draft.
                This will be drawn directly from the import file data and is
                the source of truth for the new draft. It is either a dictionary
                shaped like the record's `files.entries` property OR a list of
                dictionaries shaped like the payload for initiating file uploads via
                the low-level files API.
            files (list): the files to be uploaded to the new draft. This
                carries the files provided by the caller if we are not reading
                them from a local folder. If this is empty we will look in
                a folder for files to upload. It should be a list of FileData
                objects, which are defined in the
                `invenio_record_importer_kcworks.types` module.
            existing_record (dict): the existing record metadata, if we are
                updating a draft of a published record or an unpublished
                preexisting draft. It will only be empty if we are creating a
                new record with no preexisting drafts.
            source_filepaths (dict): a dictionary mapping file keys to the
                source file paths for the files to be uploaded. FIXME: We
                need to implement an input pathway to provide this value
                for all imports.
            clean_filenames (bool): If True, sanitize filenames before
                uploading.
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
            dict[str, FileUploadResult]: A dictionary with the new draft's
            filenames as keys. The values are FileUploadResult objects with
            'status' (str) and 'messages' (list[str]) fields.

        """
        assert metadata["files"]["enabled"] is True
        if isinstance(file_data, list):
            files_to_upload = {f["key"]: f for f in file_data}
        else:
            files_to_upload = file_data

        if clean_filenames:
            files_to_upload = self.sanitize_all_filenames(files_to_upload, files)

        uploaded_files: dict[str, FileUploadResult] = {}
        same_files = False

        if existing_record:
            same_files, files_to_upload = self._compare_existing_files(
                metadata["id"],
                existing_record["is_draft"],
                existing_record["is_published"],
                existing_record["files"]["entries"],
                files_to_upload,
            )

        if existing_record and same_files:
            app.logger.info("skipping uploading files (same already uploaded)...")

            # Check that any published record has the correct file entries
            if existing_record["is_published"]:
                check_record = records_service.read(
                    system_identity, id_=metadata["id"]
                )._record
                # assert check_record.files.entries == file_data["entries"]
                print("    published record files are...")
                print(pformat(check_record.files.entries))

            # Check that any draft record has the correct file entries
            if existing_record["is_draft"]:
                check_record = records_service.read_draft(
                    system_identity, id_=metadata["id"]
                )._record
                if (
                    uow
                    and files_to_upload
                    and check_record.files.entries != files_to_upload["entries"]
                ):
                    app.logger.error("draft record files are missing, updating...")
                    check_record.files.sync(files_to_upload["entries"])
                    uow.register(RecordCommitOp(check_record))
                print("    draft record files are...")
                print(pformat(check_record.files.entries))

            uploaded_files = {
                k: {"status": "already_uploaded", "messages": []}
                for k in files_to_upload.keys()
            }

        elif len(files_to_upload) > 0:
            app.logger.info("uploading new files...")
            # FIXME: Below is an implementation detail for the CORE
            # migration that should be removed when we use this method
            # for all imports.
            if not source_filepaths and not files:
                first_file = next(iter(files_to_upload))
                try:
                    source_filepaths = {
                        first_file: metadata["custom_fields"]["hclegacy:file_location"]
                    }
                except KeyError:
                    raise FileUploadError(
                        "No binary file data or source filepaths provided "
                        f"to upload files for {metadata['id']}"
                    )

            # If we're updating a draft of a published record, we need to
            # unlock the published record files before we can upload new
            # files.
            self._unlock_files(existing_record)

            uploaded_files = self._upload_draft_files(
                metadata["id"],
                files_to_upload,
                source_filepaths or {},
                files or [],
            )

            # Lock the files again for a published record
            self._lock_files(existing_record)
        else:
            app.logger.info("no files to upload marking as metadata-only...")
            self.set_to_metadata_only(metadata["id"])
        print("returning uploaded_files:", pformat(uploaded_files))

        return uploaded_files

    # FIXME: This method is not currently used.
    @unit_of_work()
    def _retry_file_initialization(
        self,
        draft_id: str,
        k: str,
        uow: UnitOfWork | None = None,
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
            if uow:
                uow.register(RecordCommitOp(existing_record))
            else:
                raise RuntimeError("uow is required")
            assert k not in existing_record.files.entries.keys()

            app.logger.debug(
                "...file key existed on record but was empty and was "
                "removed. This probably indicates a prior failed upload."
            )

            return True
        else:
            app.logger.error(existing_record.files.entries)
            app.logger.error(
                "file key already exists on record but is not found in draft "
                "metadata retrieved by record service"
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
        long_filename = long_filename.replace("/app/site/web/app/uploads/humcore/", "")
        try:
            assert normalize_string(filename) in normalize_string(unquote(filename))
        except AssertionError:
            app.logger.error(f"file key {filename} does not match source filename")
            raise UploadFileNotFoundError(
                f"File key from metadata {filename} not found in source "
                f"file path {filename}"
            )
        return long_filename

    def _find_file_path(self, filename: str, key: str) -> Path:
        file_path = Path(app.config["RECORD_IMPORTER_FILES_LOCATION"]) / filename
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
                    file_path = Path(unicodedata.normalize("NFD", str(file_path)))
                    assert file_path.is_file()
                except AssertionError:
                    raise UploadFileNotFoundError(
                        f"    file not found for upload {file_path}..."
                    )
        return file_path

    def _check_file_size(
        self,
        file_object: SpooledTemporaryFile | BufferedReader,
        size: int | None,
        key: str,
    ) -> None:
        if size:
            try:
                file_object.seek(0, 2)  # seek to end of file
                assert file_object.tell() == size  # check byte position
                file_object.seek(0)  # seek to beginning of file
            except AssertionError:
                raise FileUploadError(f"file size mismatch for key {key}...")
        else:
            app.logger.warning(f"file size not provided for key {key}")

    @unit_of_work()
    def _upload_draft_files(
        self,
        draft_id: str,
        files_dict: dict[str, dict],
        source_filenames: dict[str, str] = {},
        files: list[FileData] = [],
        uow: UnitOfWork | None = None,
    ) -> dict[str, FileUploadResult]:
        """Upload files to a draft record.

        :param draft_id: The ID of the draft record to upload files to.
        :param files_dict: A dictionary of file keys and their corresponding file data.
        :param source_filenames: A dictionary of file keys and their
            corresponding source filenames to be used if the files are to be
            read from a directory.
        :param files: A list of FileData objects to be uploaded to the draft record
            if the files are to be read from a list of FileData objects.
        :param uow: The unit of work to register the commit operation for the
            record if we are locking and unlocking a record
            (provided by the unit of work decorator if not provided by the caller)

        :returns: A dictionary with file keys as keys. The values are FileUploadResult
            objects with 'status' (str) and 'messages' (list[str]) fields.
        """
        output: dict[str, FileUploadResult] = {}

        # Ensure a draft exists for a published record
        try:
            check_record = records_service.read_draft(
                system_identity, id_=draft_id
            )._record
        except (NoResultFound, DraftNotCreatedError):
            records_service.edit(system_identity, id_=draft_id)

        prior_failed = False
        for k, v in files_dict.items():
            if prior_failed:  # Don't try to upload more files if one has failed
                output[k] = {
                    "status": "skipped",
                    "messages": ["Prior file upload failed."],
                }
                continue
            app.logger.debug(f"uploading file: {k}")

            try:  # initialize try/catch for single file
                output[k] = {"status": "uploaded", "messages": []}

                # first check that the binary file data is available to upload
                binary_file_data: SpooledTemporaryFile | BufferedReader | None = None
                if not files:  # we expect to find file paths in the source_filenames
                    if not source_filenames or not source_filenames[k]:
                        msg = f"No source file content or file path found for file {k}."
                        raise FileUploadError(msg)
                    else:
                        long_filename = self._sanitize_filename(source_filenames[k])
                        file_path = self._find_file_path(long_filename, k)
                        app.logger.debug(f"uploading file from path: {file_path}")

                        binary_file_data = open(file_path, "rb")
                else:  # if binary file data is provided in the files list
                    app.logger.debug(f"uploading file {k} from submitted binary data")
                    try:
                        # Use the 'key' field from the entry if available,
                        # otherwise use the dict key.
                        # The 'key' field contains the full filename with extension
                        file_key = v.get("key", k)
                        file_item = [
                            f for f in files if f.filename.split("/")[-1] == file_key
                        ][0]
                        binary_file_data = file_item.stream
                    except IndexError:
                        msg = f"File {k} not found in list of files."
                        raise FileUploadError(msg)
                    except Exception as e:
                        msg = f"Failed to upload file {k} from list: {str(e)}."
                        raise FileUploadError(msg)

                # then check that the file size is correct
                try:
                    assert binary_file_data is not None
                    binary_file_data.seek(0)
                    # check will raise FileUploadError for incorrect size (catch below)
                    self._check_file_size(
                        binary_file_data, files_dict[k].get("size"), k
                    )
                except AssertionError:
                    msg = f"File {k} has no binary file data."
                    raise FileUploadError(msg)

                # initialize the file upload
                try:
                    initialization = self.files_service.init_files(
                        system_identity, draft_id, data=[{"key": k}]
                    ).to_dict()
                    assert (
                        len([
                            e["key"] for e in initialization["entries"] if e["key"] == k
                        ])
                        == 1
                    )
                except InvalidKeyError as e:
                    msg = (
                        f"Failed to initialize file upload. Key {k} is invalid: "
                        f"{str(e)}."
                    )
                    raise FileUploadError(msg)
                except Exception as e:
                    msg = f"Failed to initialize file upload for {k}: {str(e)}."
                    raise FileUploadError(msg)

                # If a draft's file upload is interrupted, occasionally the
                # file bucket is not created.
                try:
                    check_record = records_service.read_draft(
                        system_identity, id_=draft_id
                    )._record
                    draft_files = check_record.files
                    if uow and draft_files.bucket is None:
                        draft_files.create_bucket()
                        app.logger.info("created bucket for draft files...")
                        uow.register(RecordCommitOp(check_record))
                except (NoResultFound, DraftNotCreatedError):
                    pass
                except Exception as e:
                    msg = f"Failed to get the bucket for draft files: {str(e)}."
                    raise FileUploadError(msg)

                # upload the file content
                try:
                    self.files_service.set_file_content(
                        system_identity, draft_id, k, binary_file_data
                    )
                except Exception as e:
                    msg = f"Failed to set file content for {k}: {str(e)}."
                    raise FileUploadError(msg)

                # commit the file upload
                try:
                    self.files_service.commit_file(system_identity, draft_id, k)
                except Exception as e:
                    msg = f"Failed to commit file upload for {k}: {str(e)}."
                    raise FileUploadError(msg)

            except FileUploadError as e:  # catches anticipated errors for file
                app.logger.error(e.message)
                prior_failed = True
                output[k]["status"] = "failed"
                output[k]["messages"].append(e.message)
                # clean up pending initialization
                try:
                    self.files_service.delete_file(system_identity, draft_id, k)
                except (PIDDoesNotExistError, FileKeyNotFoundError) as e:
                    # FIXME: This delete fails because recid not found. Problem?
                    app.logger.error(
                        f"Failed to delete initialized file {k} for {draft_id}."
                    )
                    app.logger.error(str(e))

            except Exception as e:  # catches unexpected errors for individual file
                prior_failed = True
                msg = f"Failed to upload file {k}: {str(e)}."
                app.logger.error(msg)
                output[k]["status"] = "failed"
                output[k]["messages"].append(msg)
                # clean up pending initialization
                try:
                    self.files_service.delete_file(system_identity, draft_id, k)
                except PIDDoesNotExistError as e:
                    # FIXME: This delete fails because recid not found. Problem?
                    app.logger.error(
                        f"Failed to delete initialized file {k} for {draft_id}."
                    )
                    app.logger.error(str(e))

        # Confirm that successful files were uploaded correctly
        if not prior_failed:  # Don't check if any files failed
            try:
                result_record = self.files_service.list_files(
                    system_identity, draft_id
                ).to_dict()
                assert all(
                    r["key"]
                    for r in result_record["entries"]
                    if r["key"] in files_dict.keys()
                )
                for v in result_record["entries"]:
                    try:
                        submitted_rec = files_dict[v["key"]]
                        if v["status"] != "completed":
                            raise FileUploadError(
                                f"File {v['key']} upload was not completed correctly "
                                f"for {draft_id}: status is {v['status']}."
                            )
                        if submitted_rec.get("size") and str(v["size"]) != str(
                            submitted_rec["size"]
                        ):
                            raise FileUploadError(
                                f"Uploaded file size ({v['size']}) does not match "
                                f"expected size ({submitted_rec['size']})"
                            )
                        if not (valid_date(v["created"]) and valid_date(v["updated"])):
                            raise FileUploadError(
                                f"File {v['key']} metadata is missing created or "
                                "updated dates."
                            )
                    except FileUploadError as e:
                        output[v["key"]]["status"] = "failed"
                        output[v["key"]]["messages"].append(str(e.message))
                        # clean up pending initialization
                        try:
                            self.files_service.delete_file(
                                system_identity, draft_id, v["key"]
                            )
                        except PIDDoesNotExistError as e:
                            # FIXME: This delete fails because recid not found. Problem?
                            app.logger.error(
                                f"Failed to delete initialized file {v['key']} "
                                f"for {draft_id}."
                            )
                            app.logger.error(str(e))
            except AssertionError as e:
                msg = (
                    f"Failed to confirm that the files were uploaded correctly:"
                    f" {str(e)}."
                )
                for k in files_dict.keys():
                    output[k]["status"] = "failed"
                    output[k]["messages"].append(msg)
            # except PIDDoesNotExistError:  # triggered by a lot of attempts to delete file
            #     for k in files_dict.keys():
            #         output[k][0] = "failed"
            #         output[k][1].append(
            #             f"Could not read uploaded files for draft {draft_id}."
            #         )

            # Handle published records without drafts, where record needs
            # file metadata from draft files service (usually synced during
            # draft publication but we're not publishing a draft here)
            try:
                check_record = records_service.read(system_identity, id_=draft_id)
                if check_record.to_dict()["files"]["entries"] == {}:
                    app.logger.info("    syncing published record files...")
                    check_draft = records_service.read_draft(
                        system_identity, id_=draft_id
                    )
                    app.logger.info("    draft record files are...")
                    app.logger.info(pformat(check_draft._record.files.entries))
                    check_record._record.files.sync(check_draft._record.files)
                    check_record._record.files.lock()
                    app.logger.info("    published record files are...")
                    app.logger.info(pformat(check_record._record.files.entries))
            except (PIDUnregistered, PIDDoesNotExistError):  # no published record
                pass

        return output

    def _compare_existing_files(
        self,
        draft_id: str,
        is_draft: bool,
        is_published: bool,
        old_files: dict[str, dict],
        new_entries: dict[str, dict],
    ) -> tuple[bool, dict[str, dict]]:
        """Compare existing files to new entries.

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
                if unicodedata.normalize("NFC", f["key"]) not in normalized_new_keys
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
            existing_published_files = published_files_request.get("entries", [])
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
            self._clear_managers(draft_id, existing_files, is_draft, is_published)

            # If there are new files to be uploaded, set same_files to False
            if len(new_entries) > 0:
                same_files = False
                files_to_upload = new_entries
                print(
                    "    new files to be uploaded that are not "
                    "present in the existing record"
                )
            else:
                files_to_upload = {}
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
            if list(files_to_upload.keys()) != list(pub_files_to_upload.keys()):
                print("draft and published files to upload are different")
                print("draft files to upload:", files_to_upload)
                print("published files to upload:", pub_files_to_upload)

        print("returning same files:", same_files)
        print("returning files to upload:", files_to_upload)
        return same_files, files_to_upload
