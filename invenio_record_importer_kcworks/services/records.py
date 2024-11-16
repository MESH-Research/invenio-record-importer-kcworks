#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2024 Mesh Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

from flask import current_app as app
from invenio_access.permissions import system_identity
from invenio_access.utils import get_identity
from invenio_accounts.models import User
from invenio_accounts.proxies import current_accounts
from invenio_db import db
from invenio_i18n.proxies import current_i18n
from invenio_pidstore.errors import PIDUnregistered, PIDDoesNotExistError
from invenio_rdm_records.records.api import RDMRecord
from invenio_rdm_records.resources.serializers.csl import (
    CSLJSONSerializer,
    get_citation_string,
    get_style_location,
)
from invenio_rdm_records.services.errors import (
    ReviewNotFoundError,
)
from invenio_rdm_records.services.pids.providers.base import PIDProvider
from invenio_rdm_records.proxies import (
    current_rdm_records,
    current_rdm_records_service as records_service,
)
from invenio_record_importer_kcworks.services.users import UsersHelper
from invenio_record_importer_kcworks.errors import (
    DraftDeletionFailedError,
    ExistingRecordNotUpdatedError,
    NoUpdates,
    PublicationValidationError,
    UpdateValidationError,
)
from invenio_record_importer_kcworks.utils.utils import (
    compare_metadata,
)
from invenio_records.systemfields.relations.errors import InvalidRelationValue
from invenio_search.proxies import current_search_client
from marshmallow.exceptions import ValidationError
from pprint import pformat
from sqlalchemy.orm.exc import NoResultFound
from typing import Optional


class RecordsHelper:
    """
    A helper class for working with Invenio records during record imports.

    Includes public methods for creating/updating records from metadata and for
    deleting records.
    """

    def __init__(self):
        pass

    @staticmethod
    def change_record_ownership(
        record_id: str,
        new_owner: User,
    ) -> dict:
        """
        Change the owner of the specified record to a new user.
        """
        app.logger.debug(f"Changing ownership of record {record_id}")

        record = records_service.read(
            id_=record_id, identity=system_identity
        )._record

        parent = record.parent
        parent.access.owned_by = new_owner
        parent.commit()
        db.session.commit()

        if records_service.indexer:
            records_service.indexer.index(record)
        result = records_service.read(
            id_=record_id, identity=system_identity
        )._record

        return result.parent.access.owned_by

    @staticmethod
    def assign_record_ownership(
        draft_id: str,
        core_data: dict,
        record_source: str,
        existing_record: Optional[dict] = None,
    ):
        # Create/find the necessary user account
        app.logger.info("    creating or finding the user (submitter)...")
        # TODO: Make sure this will be the same email used for SAML login
        new_owner_email = core_data["custom_fields"].get("kcr:submitter_email")
        new_owner_username = core_data["custom_fields"].get(
            "kcr:submitter_username"
        )
        if not new_owner_email and not new_owner_username:
            app.logger.warning(
                "    no submitter email or username found in source metadata. "
                "Assigning ownership to configured admin user..."
            )
            new_owner_email = app.config["RECORD_IMPORTER_ADMIN_EMAIL"]
            new_owner_username = None
        full_name = ""
        for c in [
            *core_data["metadata"].get("creators", []),
            *core_data["metadata"].get("contributors", []),
        ]:
            for i in c["person_or_org"].get("identifiers", []):
                if i["scheme"] == "hc_username":
                    full_name = c["person_or_org"]["name"]
        existing_user = current_accounts.datastore.get_user_by_email(
            new_owner_email
        )
        if not existing_user:
            # handle case where user has multiple emails
            try:
                existing_user = current_accounts.datastore.find_user(
                    username=f"{record_source.lower()}-{new_owner_username}",
                )
                app.logger.warning(
                    f"    finding existing user {new_owner_username} "
                    f"({new_owner_email})...{existing_user}"
                )
                print(
                    f"    finding existing user {new_owner_username} "
                    f"({new_owner_email})...{existing_user}"
                )
                assert existing_user
                idp_slug = (
                    "kc"
                    if record_source == "knowledgeCommons"
                    else record_source
                )
                existing_user.user_profile[
                    f"identifier_{idp_slug}_username"
                ] = new_owner_username
                existing_user.user_profile["identifier_email"] = (
                    new_owner_email
                )
                current_accounts.datastore.commit()
            except (NoResultFound, AssertionError):
                pass
        if existing_user:
            new_owner = existing_user
            app.logger.debug(
                f"    assigning ownership to existing user: "
                f"{pformat(existing_user)} {existing_user.email}"
            )
        else:
            new_owner_result = UsersHelper().create_invenio_user(
                new_owner_email, new_owner_username, full_name, record_source
            )
            new_owner = new_owner_result["user"]
            app.logger.info(f"    new user created: {pformat(new_owner)}")

        # if existing_record:
        #     app.logger.debug("existing record data")
        # app.logger.debug(
        #     existing_record["custom_fields"]["kcr:submitter_email"]
        # )
        # app.logger.debug(existing_record["parent"])
        if (
            existing_record
            and existing_record["custom_fields"].get("kcr:submitter_email")
            == new_owner_email
            and existing_record["parent"]["access"].get("owned_by")
            and str(existing_record["parent"]["access"]["owned_by"]["user"])
            == str(new_owner.id)
        ):
            app.logger.info(
                "    skipping re-assigning ownership of the record "
            )
            app.logger.info(
                f"    (already belongs to {new_owner_email}, "
                f"user {new_owner.id})..."
            )
        else:
            # Change the ownership of the record
            app.logger.info(
                "    re-assigning ownership of the record to the "
                f"submitter ({new_owner_email}, "
                f"{new_owner.id})..."
            )
            changed_ownership = RecordsHelper.change_record_ownership(
                draft_id, new_owner
            )
            # Remember: changed_ownership is an Owner systemfield object,
            # not User
            assert changed_ownership.owner_id == new_owner.id
        return new_owner

    @staticmethod
    def _coerce_types(metadata: dict) -> dict:
        """
        Coerce metadata values to the correct types.

        This is necessary for integer fields, since the metadata
        is stored as a JSON string and so all values are strings.
        """
        # FIXME: Replace this with a proper loader
        if metadata["custom_fields"].get("hclegacy:total_downloads"):
            metadata["custom_fields"]["hclegacy:total_downloads"] = int(
                metadata["custom_fields"]["hclegacy:total_downloads"]
            )
        if metadata["custom_fields"].get("hclegacy:total_views"):
            metadata["custom_fields"]["hclegacy:total_views"] = int(
                metadata["custom_fields"]["hclegacy:total_views"]
            )
        return metadata

    def create_invenio_record(
        self,
        metadata: dict,
        no_updates: bool = False,
    ) -> dict:
        """
        Create a new Invenio record from the provided dictionary of metadata.

        If no record with the same DOI exists, a new draft record is created
        and left unpublished. If a record with the same DOI does exist, we
        compare the existing record's metadata to the new record's metadata. If
        the metadata has changed, we update the existing record if `no_updates`
        flag is not True.

        Note that we assume any overrides to the metadata have already been
        applied before this function is called.

        params:
            metadata (dict): the metadata for the new record
            no_updates (bool): whether to update an existing record with the
                same DOI if it exists (default: False)

        returns:
            dict: a dictionary containing the status of the record creation
                and the record data. The keys are

                'status': The kind of record operation that produced the new/
                    current metadata record. Possible values: 'new_record',
                    'updated_draft', 'updated_published',
                    'unchanged_existing_draft',
                    'unchanged_existing_published'
                'record_data': The metadata record created or updated
                    by the operation.
                'recid': The record internal UUID for the created record

            It contains the following keys:
            - record_data: the metadata record
            - record_uuid: the UUID of the metadata record
            - status: the status of the metadata record
        """
        app.logger.debug("~~~~~~~~")
        metadata = RecordsHelper._coerce_types(metadata)
        app.logger.debug("metadata for new record:")
        app.logger.debug(pformat(metadata))

        # Check for existing record with same DOI
        if "pids" in metadata.keys() and "doi" in metadata["pids"].keys():
            my_doi = metadata["pids"]["doi"]["identifier"]
            doi_for_query = my_doi.split("/")
            # TODO: Can we include deleted records here somehow?
            try:
                print(f"searching for existing record with DOI: {my_doi}")
                print(
                    f"query: q=f'pids.doi.identifier:{doi_for_query[0]}/"
                    f"{doi_for_query[1]}'"
                )

                same_doi = current_search_client.search(
                    index="kcworks-rdmrecords",
                    q=f'pids.doi.identifier:"{doi_for_query[0]}/'
                    f'{doi_for_query[1]}"',
                )
                # same_doi = records_service.search_drafts(
                #     system_identity,
                #     q=f"pids.doi.identifier:{doi_for_query[0]}/"
                #     f"{doi_for_query[1]}",
                # )
                app.logger.debug(f"same_doi: {pformat(same_doi)}")
            except Exception as e:
                app.logger.error(
                    "    error checking for existing record with same DOI:"
                )
                raise e
            if same_doi["hits"]["total"]["value"] > 0:
                draft_recs = []
                published_recs = []
                rec_ids = [
                    r["_source"]["id"] for r in same_doi["hits"]["hits"]
                ]
                for rec_id in rec_ids:
                    try:
                        published_rec = records_service.read(
                            system_identity, id_=rec_id
                        ).to_dict()
                        if (
                            published_rec["pids"]["doi"]["identifier"]
                            != my_doi
                        ):
                            published_doi = published_rec["pids"]["doi"][
                                "identifier"
                            ]
                            print(
                                f"published rec doi for {rec_id} ("
                                f"{published_doi}) does not actually match "
                                f"doi for new record ({my_doi}). "
                                "draft was corrupted."
                            )
                            pass  # Don't use this record
                        else:
                            published_recs.append(published_rec)
                    except PIDUnregistered:
                        draft_recs.append(
                            records_service.read_draft(
                                system_identity, id_=rec_id
                            ).to_dict()
                        )
                    except KeyError:
                        # FIXME: indicates missing published record for
                        # registered PID; we try to delete PID locally
                        # and ignore the corrupted draft
                        provider = PIDProvider(
                            "base", client=None, pid_type="doi"
                        )
                        stranded_pid = provider.get(
                            metadata["pids"]["doi"]["identifier"]
                        )
                        stranded_pid.status = "N"
                        stranded_pid.delete()
                raw_recs = published_recs + draft_recs
                recs = []
                # deduplicate based on invenio record id and prefer published
                # records over drafts
                for r in raw_recs:
                    if r["id"] not in [p["id"] for p in recs]:
                        recs.append(r)

                app.logger.info(
                    f"    found {same_doi['hits']['total']['value']} existing"
                    " records with same DOI..."
                )
                # check for corrupted draft with different DOI and add to recs
                # so that we can delete it
                try:
                    existing_draft_hit = records_service.search_drafts(
                        system_identity,
                        q=f"id:{rec_ids[0]}",
                    )._results[0]
                    if (
                        existing_draft_hit.to_dict()["pids"]["doi"][
                            "identifier"
                        ]
                        != my_doi
                    ):
                        recs.append(existing_draft_hit.to_dict())
                except IndexError:
                    pass
                # delete extra records with the same doi
                if len(recs) > 1:
                    rec_list = [
                        (
                            r["id"],
                            ("published" if r["is_published"] else "draft"),
                        )
                        for r in recs
                    ]
                    app.logger.info(
                        "    found more than one existing record with same "
                        f"DOI: {rec_list}"
                    )
                    app.logger.info("   deleting extra records...")
                    for r in recs[1:]:
                        try:
                            self.delete_invenio_record(
                                r["id"], record_type="draft"
                            )
                        except PIDUnregistered as e:
                            app.logger.error(
                                f"    error deleting extra record with same "
                                f"DOI: {r['id']} was unregistered: {str(e)}"
                            )
                            raise DraftDeletionFailedError(
                                f"Draft deletion failed because PID for "
                                f"record {r['id']} was unregistered: {str(e)}"
                            )
                        except Exception as e:
                            if r["is_published"] and not r["is_draft"]:
                                app.logger.error(
                                    f"    error deleting extra published "
                                    f"record {r['id']} with same DOI: {str(e)}"
                                )
                                raise DraftDeletionFailedError(
                                    f"Draft deletion failed for published "
                                    f"record {r['id']} with same DOI: {str(e)}"
                                )
                            else:
                                app.logger.info(
                                    f"    could not delete draft record "
                                    f"{r['id']} with same DOI. It will be "
                                    "cleaned up by the system later."
                                )
                                pass
                existing_metadata = recs[0] if len(recs) > 0 else None
                # app.logger.debug(
                #     f"existing_metadata: {pformat(existing_metadata)}"
                # )
                # Check for differences in metadata
                if existing_metadata:
                    differences = compare_metadata(existing_metadata, metadata)
                    app.logger.debug(f"differences: {differences}")
                    if differences:
                        app.logger.info(
                            "    existing record with same DOI has different"
                            f" metadata: existing record: {differences['A']}"
                            f"; new record: {differences['B']}"
                        )
                        if no_updates:
                            raise NoUpdates(
                                "no_updates flag is set, so not updating "
                                "existing record with changed metadata..."
                            )
                        update_payload = existing_metadata.copy()
                        for key, val in differences["B"].items():
                            if key in [
                                "access",
                                "custom_fields",
                                "files",
                                "metadata",
                                "pids",
                            ]:
                                for k2 in val.keys():
                                    if val[k2] is None:
                                        update_payload.setdefault(key, {}).pop(
                                            k2
                                        )
                                    else:
                                        update_payload.setdefault(key, {})[
                                            k2
                                        ] = metadata[key][k2]
                        app.logger.info(
                            "    updating existing record with new metadata..."
                        )
                        new_comparison = compare_metadata(
                            existing_metadata, update_payload
                        )
                        if new_comparison:
                            app.logger.debug(
                                f"existing record: "
                                f"{pformat(new_comparison['A'])}"
                                "new record:"
                                f" {pformat(new_comparison['B'])}"
                            )
                            raise ExistingRecordNotUpdatedError(
                                "    metadata still does not match migration "
                                "source after update attempt..."
                            )
                        else:
                            update_payload = {
                                k: v
                                for k, v in update_payload.items()
                                if k
                                in [
                                    "access",
                                    "custom_fields",
                                    "files",
                                    "metadata",
                                    "pids",
                                ]
                            }
                            # TODO: Check whether this is the right way
                            # to update
                            if existing_metadata["files"].get("enabled") and (
                                len(
                                    existing_metadata["files"][
                                        "entries"
                                    ].keys()
                                )
                                > 0
                            ):
                                # update_draft below will copy files from the
                                # existing draft's file manager over to the new
                                # draft's file manager. We need *some* files in
                                # the metadata here to avoid a validation
                                # error, but it will be overwritten by the
                                # files in the file manager. We have to use
                                # the existing draft's files here to avoid
                                # problems setting the default files for the
                                # new draft in
                                # BaseRecordFilesComponent.update_draft
                                app.logger.info(
                                    "    existing record has files attached..."
                                )
                                update_payload["files"] = existing_metadata[
                                    "files"
                                ]
                                # update_payload["files"] = metadata["files"]
                                print(
                                    f"update_payload['files']: "
                                    f"{pformat(update_payload['files'])}"
                                )
                            # Invenio validator will reject other
                            # rights metadata values from existing records
                            if existing_metadata["metadata"].get("rights"):
                                existing_metadata["metadata"]["rights"] = [
                                    {"id": r["id"]}
                                    for r in existing_metadata["metadata"][
                                        "rights"
                                    ]
                                ]
                            app.logger.info(
                                "    metadata updated to match migration "
                                "source..."
                            )
                            try:
                                # If there is an existing draft for a
                                # published record, or an unpublished draft,
                                # we update the draft
                                result = records_service.update_draft(
                                    system_identity,
                                    id_=existing_metadata["id"],
                                    data=update_payload,
                                )
                                app.logger.info(
                                    "    continuing with existing draft record"
                                    " (new metadata)..."
                                )
                                app.logger.debug(pformat(result))
                                return {
                                    "status": "updated_draft",
                                    "record_data": result.to_dict(),
                                    "record_uuid": result._record.id,
                                }
                            except (PIDDoesNotExistError, NoResultFound):
                                # If there is no existing draft for the
                                # published record, we create a new draft
                                # to edit
                                app.logger.info(
                                    "    creating new draft of published "
                                    "record or recovering unpublished draft..."
                                )
                                # app.logger.debug(pprint(existing_metadata))
                                # rec = records_service.read(
                                #     system_identity,
                                #     id_=existing_metadata["id"]
                                # )
                                create_draft_result = records_service.edit(
                                    system_identity,
                                    id_=existing_metadata["id"],
                                )
                                app.logger.info(
                                    "    updating new draft of published "
                                    "record with new metadata..."
                                )
                                result = records_service.update_draft(
                                    system_identity,
                                    id_=create_draft_result.id,
                                    data=update_payload,
                                )
                                result = records_service.update_draft(
                                    system_identity,
                                    id_=create_draft_result.id,
                                    data=update_payload,
                                )
                                if result.to_dict().get("errors"):
                                    # NOTE: some validation errors don't
                                    # prevent the update and aren't indicative
                                    # of actual problems
                                    errors = [
                                        e
                                        for e in result.to_dict()["errors"]
                                        if e.get("field")
                                        != "metadata.rights.0.icon"
                                        and e.get("messages")
                                        != ["Unknown field."]
                                        and "Missing uploaded files"
                                        not in e.get("messages")[0]
                                    ]
                                    if errors:
                                        raise UpdateValidationError(
                                            f"Validation error when trying to "
                                            f"update existing record: "
                                            f"{pformat(errors)}"
                                        )
                                app.logger.info(
                                    f"updated new draft of published: "
                                    f"{pformat(result.to_dict())}"
                                )
                                app.logger.debug(
                                    f"****title: "
                                    f"{result.to_dict()['metadata'].get('title')}"  # noqa: E501
                                )
                                return {
                                    "status": "updated_published",
                                    "record_data": result.to_dict(),
                                    "record_uuid": result._record.id,
                                }

                    if not differences:
                        record_type = (
                            "draft"
                            if existing_metadata["status"] != "published"
                            else "published"
                        )
                        app.logger.info(
                            f"    continuing with existing {record_type} "
                            "record (same metadata)..."
                        )
                        existing_record_id = ""
                        try:
                            existing_record_hit = (
                                records_service.search_drafts(
                                    system_identity,
                                    q=f"id:{existing_metadata['id']}",
                                )._results[0]
                            )
                            print(
                                f"existing_record_hit draft: "
                                f"{pformat(existing_record_hit.to_dict()['pids'])}"  # noqa: E501
                            )
                            existing_record_id = existing_record_hit.to_dict()[
                                "uuid"
                            ]
                        except IndexError:
                            existing_record_hit = records_service.read(
                                system_identity, id_=existing_metadata["id"]
                            )
                            existing_record_id = existing_record_hit.id
                        result = {
                            "record_data": existing_metadata,
                            "status": f"unchanged_existing_{record_type}",
                            "record_uuid": existing_record_id,
                        }
                        app.logger.debug(
                            f"metadata for existing record: {pformat(result)}"
                        )
                        return result

        # Make draft and publish
        app.logger.info("    creating new draft record...")
        try:
            result = records_service.create(system_identity, data=metadata)
        except InvalidRelationValue as e:
            raise PublicationValidationError(
                f"Validation error while creating new record: {str(e)}"
            )
        result_recid = result._record.id
        app.logger.debug(f"    new draft record recid: {result_recid}")
        app.logger.debug(f"    new draft record: {pformat(result.to_dict())}")
        print(f"    new draft record: {pformat(result.to_dict())}")

        return {
            "status": "new_record",
            "record_data": result.to_dict(),
            "record_uuid": result_recid,
        }

    def delete_invenio_record(
        self, record_id: str, record_type: Optional[str] = None
    ) -> bool:
        """
        Delete an Invenio record with the provided Id

        params:
            record_id (str): the id string for the Invenio record
            record_type (str): the type of record to delete (default: None)

        Since drafts cannot be deleted if they have an associated review
        request, this function first deletes any existing review request for
        the draft record.

        Note: This function only works for draft (unpublished) records.

        returns:
            bool: True if the record was deleted, False otherwise

        """
        result = None
        app.logger.info(
            f"    deleting {record_type if record_type else ''} record "
            f"{record_id}..."
        )

        def inner_delete_draft(record_id: str) -> dict:
            try:  # unregistered DOI can be deleted
                result = records_service.delete_draft(
                    system_identity, id_=record_id
                )
            # TODO: if DOI is registered or reserved, but no published version
            # exists, the draft can't be manually deleted (involves deleting
            # DOI from PID registry). We let the draft be cleaned up by the
            # system after a period of time.
            except ValidationError as e:
                if (
                    "Cannot discard a reserved or registered persistent "
                    "identifier" in str(e)
                ):
                    app.logger.warning(
                        f"Cannot delete draft record {record_id} "
                        "immediately because its DOI is reserved "
                        "or registered. It will be left for later "
                        "cleanup."
                    )
                else:
                    raise e
            return result

        try:
            reviews = records_service.review.read(
                system_identity, id_=record_id
            )
            if reviews:
                # FIXME: What if there are multiple reviews?
                app.logger.debug(
                    f"    deleting review request for draft record "
                    f"{record_id}..."
                )
                records_service.review.delete(system_identity, id_=record_id)
        except ReviewNotFoundError:
            app.logger.info(
                f"    no review requests found for draft record "
                f"{record_id}..."
            )

        if record_type == "draft":
            result = inner_delete_draft(record_id)
        else:
            try:  # In case the record is actually published
                result = records_service.delete_record(
                    system_identity,
                    id_=record_id,
                    data={"note": "duplicate record for same DOI"},
                )
            except PIDUnregistered:  # this draft not published
                result = inner_delete_draft(record_id)

        return result

    def delete_records_from_invenio(record_ids, visible, reason, note):
        """
        Delete the selected records from the invenioRDM instance.

        FIXME: Amalgamate with delete_invenio_record
        """
        deleted_records = {}
        for record_id in record_ids:
            admin_email = app.config["RECORD_IMPORTER_ADMIN_EMAIL"]
            admin_identity = get_identity(
                current_accounts.datastore.get_user(admin_email)
            )
            service = current_rdm_records.records_service
            record = service.read(
                id_=record_id, identity=system_identity
            )._record
            siblings = list(RDMRecord.get_records_by_parent(record.parent))
            app.logger.warning("siblings: %s", pformat(siblings))
            # remove the 0th (latest) version to leave the previous version(s):
            siblings.pop(0)
            app.logger.warning("siblings after pop: %s", pformat(siblings))
            # already deleted previous versions will have nothing for metadata
            # (sibling.get('id') will return nothing)
            has_versions = any([sibling.get("id") for sibling in siblings])

            if record.versions.is_latest and has_versions:
                raise Exception(
                    "Cannot delete the latest version without first deleting "
                    "previous versions"
                )

            payload = {
                "removal_reason": {"id": reason},
                "is_visible": visible,
            }

            default_citation_style = app.config.get(
                "RDM_CITATION_STYLES_DEFAULT", "apa"
            )
            serializer = CSLJSONSerializer()
            style = get_style_location(default_citation_style)
            default_citation = get_citation_string(
                serializer.dump_obj(record),
                record.pid.pid_value,
                style,
                locale=current_i18n.language,
            )
            payload["citation_text"] = default_citation

            if note:
                payload["note"] = note

            deleted = service.delete_record(
                admin_identity, id_=record_id, data=payload
            )
            deleted_records[record_id] = deleted

        return deleted_records
