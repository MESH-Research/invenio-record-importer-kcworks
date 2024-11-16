from flask import current_app as app
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
from invenio_records_resources.services.errors import (
    FileKeyNotFoundError,
)
from invenio_records_resources.services.uow import (
    unit_of_work,
    UnitOfWork,
    RecordCommitOp,
)
from pprint import pformat
from pathlib import Path
from sqlalchemy.orm.exc import NoResultFound
from typing import Optional
import unicodedata
from urllib.parse import unquote


class FilesHelper:
    def __init__(self, is_draft: bool):
        self.files_service = (
            records_service.draft_files if is_draft else records_service.files
        )

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
        uow: Optional[UnitOfWork] = None,
    ):
        """
        Handle the files for a record.

        metadata (dict): the record metadata
        file_data (dict): the file data to be uploaded
        existing_record (dict): the existing record metadata, if we are
            updating a draft of a published record or an unpublished
            preexisting draft. It will only be empty if we are creating a
            new record with no preexisting drafts.

        NOTE: The files in `metadata` and `existing_record` will often be
        different. `existing_record` may have files that were already on any pre-existing published or draft record. This is the
        case *even if* we tried to update the file data during creation of
        the new draft we're editing, because the new draft creation process
        copies the files from the existing record over to the new draft.
        If we are creating a brand new record, with no prior drafts, `metadata`
        will have an empty files property. If there is an existing draft,
        `metadata` will have the same files as `existing_record`. This means
        that only `file_data` provides the file data for the current import.

        """
        print(f"handle_record_files metadata: {pformat(metadata)}")
        print(f"handle_record_files file_data: {pformat(file_data)}")
        print(
            f"handle_record_files existing_record.files: "
            f"{pformat(existing_record.get('files') if existing_record else None)}"
        )
        assert metadata["files"]["enabled"] is True
        uploaded_files = {}
        same_files = False

        if existing_record:
            same_files = self._compare_existing_files(
                metadata["id"],
                existing_record["is_draft"] is True
                and existing_record["is_published"] is False,
                existing_record["files"]["entries"],
                file_data["entries"],
            )

        if same_files:
            app.logger.info(
                "    skipping uploading files (same already uploaded)..."
            )
        else:
            app.logger.info("    uploading new files...")
            app.logger.warning("file data: %s", pformat(file_data))
            first_file = next(iter(file_data["entries"]))

            # If we're updating a draft of a published record, we need to
            # unlock the published record files before we can upload new
            # files.
            need_to_unlock = existing_record.get("is_published")
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
                file_data["entries"],
                {
                    first_file: metadata["custom_fields"][
                        "hclegacy:file_location"
                    ]
                },
            )

            if need_to_unlock:
                record.files.lock()
                uow.register(RecordCommitOp(record))

        return uploaded_files

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
            except InvalidKeyError as e:  # file with same key already exists
                raise e
                # FIXME: do we need this retry any more?
                app.logger.error(f"handling InvalidKeyError: {e}")
                self._retry_file_initialization(draft_id, k)

                try:
                    initialization = self.files_service.init_files(
                        system_identity, draft_id, data=[{"key": k}]
                    ).to_dict()
                    app.logger.debug(
                        f"initialization: {pformat(initialization)}"
                    )
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
                    app.logger.debug(
                        f"initialization.entries keys: "
                        f"{[e['key'] for e in initialization['entries']]}"
                    )
                except Exception as e:
                    app.logger.error(
                        f"    failed to initialize file upload for {draft_id}"
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

        # Handle drafts of published records, where record needs file metadata
        # from draft files service (usually synced during draft publication
        # but we're not publishing a draft here)
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
        old_files: dict[str, dict],
        new_entries: dict[str, dict],
    ) -> bool:
        """
        Compare existing files to new entries.

        draft_id (str): id of the draft record we're checking. (Although this
            will always be a draft record, it may be a draft revising a
            previously published record.)
        is_draft (bool): True if working with a draft record that has never
            been published, False if checking a draft of a published record.
        old_files (dict): existing files on the previously saved record
            metadata. This could be a prior published record or a prior
            unpublished draft. It will only be empty for a new record with no
            prior drafts. (Dict is shaped like the "entries" key of a record's
            files property.)
        new_entries (dict): new files to be uploaded to the record. (Dict is
            shaped like the "entries" key of a record's files property.)

        If files are different, delete the wrong files from the record. If
        files are missing the file services and/or the draft metadata, ensure
        that the files are deleted from the draft record's file manager.

        Note that there are separate files services for published and draft
        records. We have to use the correct one for the record we're
        checking.

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
          the draft record's file manager. We still return False.  FIXME: Do we need to return False here? We don't have to upload anything.
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

        FIXME: Do we ever have a record that is not a draft version, even
        if it has been published prior? Yes, if we're checking an unchanged
        published record? But in that case we're not changing anything?

        Return True if files are the same, False otherwise.

        """
        # files_service = (
        #     records_service.files
        #     if not is_draft
        #     else records_service.draft_files
        # )
        print("is_draft:", is_draft)
        same_files = True

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

        for existing_files, files_service in [
            (existing_draft_files, records_service.draft_files),
            (existing_published_files, records_service.files),
        ]:
            print("files service:", files_service)
            print("existing files:", existing_files)
            print("new entries:", new_entries)
            if len(existing_files) == 0 or old_files == {}:
                if len(new_entries) > 0:
                    same_files = False

                record = files_service._get_record(
                    draft_id, system_identity, "delete_files"
                )
                print("record.files.entries:", record.files.entries)
                if record.files.entries:
                    record.files.unlock()
                    record.files.delete_all(
                        remove_obj=True, softdelete_obj=False, remove_rf=True
                    )
                    app.logger.info(
                        "    deleted all files from existing record"
                    )
                else:
                    app.logger.info("    no files attached to existing record")
            else:
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

                for k, v in new_entries.items():
                    wrong_file = False
                    existing_file = [
                        f
                        for f in existing_files
                        if unicodedata.normalize("NFC", f["key"])
                        == unicodedata.normalize("NFC", k)
                    ]

                    if len(existing_file) == 0:
                        same_files = False
                        print("no existing file found")

                    elif (existing_file[0]["status"] == "pending") or (
                        str(v["size"]) != str(existing_file[0]["size"])
                    ):
                        same_files = False
                        wrong_file = True
                        print("pending or size mismatch")

                    if wrong_file:
                        self._delete_file(
                            draft_id,
                            existing_file[0]["key"],
                            files_service=files_service,
                        )

            print("same files:", same_files)
            return same_files
