"""REST resources for the record importer API."""

import json
import mimetypes
import zipfile
from tempfile import SpooledTemporaryFile

import marshmallow as ma
from flask import Response, g, jsonify
from flask import current_app as app
from flask_resources import Resource, ResourceConfig
from flask_resources.config import from_conf
from flask_resources.context import resource_requestctx
from flask_resources.deserializers import JSONDeserializer
from flask_resources.parsers.body import RequestBodyParser
from flask_resources.parsers.decorators import request_parser
from flask_resources.resources import route
from flask_resources.responses import ResponseHandler
from flask_resources.serializers.json import JSONSerializer
from invenio_oauth2server.decorators import require_api_auth, require_oauth_scopes
from invenio_records_resources.services.errors import PermissionDeniedError
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.exceptions import (
    BadRequest,
    Conflict,
    Forbidden,
    MethodNotAllowed,
    NotFound,
    UnprocessableEntity,
)
from werkzeug.utils import secure_filename

from .errors import (
    ActiveImportExistsError,
    ImportCommunityMismatchError,
    ImportNotFoundError,
)
from .parser import RequestMultipartParser, request_body_parser
from .types import FileData


def bool_from_string(value: str) -> bool:
    """Convert a string to a boolean.

    If the value is already a boolean, just return it. If the value is not a
    standard string representation of a boolean, raise a BadRequest exception.

    Returns:
        Description of the return value.

    Raises:
        BadRequest: Raised when the operation fails.
    """
    if value in ["true", "True", "TRUE", "1", True]:
        return True
    elif value in ["false", "False", "FALSE", "0", False]:
        return False
    else:
        raise BadRequest(f"Invalid boolean value: {value}")


def is_zip_file(file) -> bool:
    """Check if a file is a zip archive.

    Args:
        file: A werkzeug FileStorage object.

    Returns:
        True if the file is a zip archive, False otherwise.
    """
    if file.filename and file.filename.lower().endswith(".zip"):
        return True
    if file.mimetype and file.mimetype == "application/zip":
        return True
    if file.content_type and file.content_type == "application/zip":
        return True
    return False


def extract_zip_files(zip_file) -> list[FileData]:
    """Extract files from a zip archive.

    Args:
        zip_file: A werkzeug FileStorage object containing a zip archive.

    Returns:
        A list of FileData objects for each file in the zip archive.

    Raises:
        BadRequest: If the zip file is invalid or contains subfolders.
    """
    extracted_files = []

    try:
        zip_file.stream.seek(0)

        with zipfile.ZipFile(zip_file.stream, "r") as zip_archive:
            file_names = zip_archive.namelist()

            if not file_names:
                raise BadRequest("Zip archive is empty")

            # Check for subfolders - files must be in a single folder with no subfolders
            root_dirs = set()
            for file_name in file_names:
                if file_name.endswith("/"):
                    continue

                # Split the path to check for subfolders
                parts = file_name.split("/")
                if len(parts) > 1:
                    root_dir = parts[0]
                    root_dirs.add(root_dir)
                    if len(parts) > 2:
                        raise BadRequest(
                            f"Zip archive contains subfolders. "
                            "Files must be in a single compressed folder "
                            "with no subfolders. "
                            f"Found: {file_name}"
                        )

            # If all files are in a single root directory, extract them
            # Otherwise, if files are at the root, extract them directly
            if len(root_dirs) > 1:
                raise BadRequest(
                    f"Zip archive contains files in multiple folders. "
                    f"Files must be in a single compressed folder. "
                    f"Found folders: {', '.join(root_dirs)}"
                )

            chunk_size = 8 * 1024 * 1024  # 8 MiB
            for file_name in file_names:
                # Skip directory entries
                if file_name.endswith("/"):
                    continue

                # Stream member bytes into a spool instead of zip_archive.read()
                # which loads the whole member into memory.
                extracted_stream = SpooledTemporaryFile(
                    max_size=10 * 1024 * 1024
                )  # 10MB before rolling to disk
                with zip_archive.open(file_name, "r") as member:
                    while True:
                        chunk = member.read(chunk_size)
                        if not chunk:
                            break
                        extracted_stream.write(chunk)
                extracted_stream.seek(0)

                display_filename = (
                    file_name.split("/")[-1] if "/" in file_name else file_name
                )

                mimetype, _ = mimetypes.guess_type(display_filename)
                content_type = mimetype or "application/octet-stream"

                extracted_file = FileData(
                    filename=secure_filename(display_filename),
                    content_type=content_type,
                    mimetype=content_type,
                    mimetype_params={},
                    stream=extracted_stream,
                )

                extracted_files.append(extracted_file)

        zip_file.stream.seek(0)

    except zipfile.BadZipFile:
        raise BadRequest(f"Invalid zip archive: {zip_file.filename}") from None
    except Exception as e:
        if isinstance(e, BadRequest):
            raise
        raise BadRequest(f"Error extracting zip archive: {str(e)}") from e

    return extracted_files


# Decorators

request_form_data = request_body_parser(
    parsers=from_conf("request_body_parsers"),
    default_content_type=str(from_conf("default_content_type")),
)

request_parsed_view_args = request_parser(
    {
        "community": ma.fields.String(),
        "import_id": ma.fields.String(required=False),
    },
    location="view_args",
)


class RecordImporterResourceConfig(ResourceConfig):
    """RecordImporterResourceConfig."""

    blueprint_name = "record_importer_kcworks"

    url_prefix = "/import"

    # Error handlers are registered on the blueprint by flask_resources
    # using the `Blueprint.register_error_handler()` method.
    error_handlers = {
        Forbidden: lambda e: (
            {"message": str(e), "status": 403},
            403,
        ),
        PermissionDeniedError: lambda e: (
            {
                "message": (
                    "The user does not have the necessary permissions to "
                    "import records via this endpoint."
                ),
                "status": 403,
            },
            403,
        ),
        MethodNotAllowed: lambda e: (
            {"message": str(e), "status": 405},
            405,
        ),
        NotFound: lambda e: (
            {"message": str(e), "status": 404},
            404,
        ),
        Conflict: lambda e: (
            {"message": str(e), "status": 409},
            409,
        ),
        ActiveImportExistsError: lambda e: (
            {
                "message": str(e),
                "status": 409,
                "existing_import_id": getattr(e, "existing_import_id", None),
            },
            409,
        ),
        ImportNotFoundError: lambda e: (
            {"message": str(e), "status": 404},
            404,
        ),
        ImportCommunityMismatchError: lambda e: (
            {"message": str(e), "status": 404},
            404,
        ),
        BadRequest: lambda e: (
            {"message": str(e), "status": 400},
            400,
        ),
        json.JSONDecodeError: lambda e: (
            {
                "message": (
                    f"Invalid metadata JSON: {e.msg} "
                    f"(line {e.lineno} column {e.colno})"
                ),
                "status": 400,
            },
        ValueError: lambda e: (
            {"message": str(e), "status": 400},
            400,
        ),
        ma.ValidationError: lambda e: (
            {"message": str(e), "status": 400},
            400,
        ),
        UnprocessableEntity: lambda e: (
            {"message": str(e), "status": 422},
            422,
        ),
        RuntimeError: lambda e: (
            {"message": str(e), "status": 500},
            500,
        ),
        NotImplementedError: lambda e: (
            {"message": str(e), "status": 501},
            501,
        ),
    }

    # Request parsing
    default_content_type = "multipart/form-data"
    request_body_parsers = {
        "multipart/form-data": RequestMultipartParser(),
        "application/json": RequestBodyParser(deserializer=JSONDeserializer()),
    }

    # Response handling
    default_accept_mimetype = "application/json"
    response_handlers = {"application/json": ResponseHandler(JSONSerializer())}


class RecordImporterResource(Resource):
    """RecordImporterResource."""

    def __init__(self, config, service):
        """Initialize the instance."""
        super().__init__(config)
        self.service = service

    def create_url_rules(self):
        """Create the URL rules for the record resource.

        Registered by flask_resources on the blueprint using the
        `Blueprint.add_url_rule()` method.


        Returns:
            Description of the return value.
        """
        return [
            route("POST", "/<community>", self.import_records),
            # Register `/current` before `/<import_id>`.
            route("GET", "/<community>/current", self.get_current_import),
            route("DELETE", "/<community>/current", self.cancel_current_import),
            route("GET", "/<community>", self.list_imports),
            route("GET", "/<community>/<import_id>", self.get_import),
            route("DELETE", "/<community>/<import_id>", self.cancel_import),
        ]

    @request_parsed_view_args
    @request_form_data
    @require_api_auth()
    @require_oauth_scopes("records:import")
    def import_records(self):
        """Create an async import (v2) or run the legacy sync import.

        Path: `POST /api/import/<community>` (community UUID or slug).

        **v2 — `application/json`:** creates a `RecordImport` row
        (`awaiting_files`) and returns **202**. No file bytes on this request.
        Body fields:

        - `api_version` (int, optional): defaults to `2` when omitted; if
          present, must be `2`
        - `metadata` (list): record metadata objects to ingest after commit
        - `review_required` (bool)
        - `strict_validation` (bool)
        - `all_or_none` (bool)
        - `no_updates` (bool)
        - `notify_record_owners` (bool)
        - `id_scheme` (str)
        - `alternate_id_scheme` (str)

        Flags (everything except `metadata`) are stored on the import
        `options` column (`api_version` recorded as `2` when omitted).

        **Legacy — `multipart/form-data`:** sync import; see
        `_import_records_legacy` for the field contract.

        Returns:
            Flask `(response, status)` for create (202) or sync import.
        """
        community_id = resource_requestctx.view_args.get("community")
        data = resource_requestctx.data

        # JSON body → async create (v2). Multipart → legacy sync.
        if isinstance(data, dict) and "form" not in data and "files" not in data:
            return self._create_import_v2(community_id, data)

        return self._import_records_legacy(community_id, data)

    def _create_import_v2(self, community_id: str, data: dict):
        """Handle JSON create for the async import protocol.

        Omitting `api_version` defaults to v2. An explicit non-2 value is
        rejected (use multipart for the legacy sync path).

        Args:
            community_id: Community UUID or slug from the URL.
            data: Parsed JSON body.

        Returns:
            `(jsonify(body), 202)` where `body` is a `RecordImport.to_dict()`
            payload for the new row, typically:

            - `id` (str): import UUID
            - `community_id` (str): resolved community UUID
            - `created_by` (int): creating user id
            - `status` (str): `"awaiting_files"`
            - `records_metadata` (list): echo of request `metadata`
            - `options` (dict): flags including `api_version: 2`
            - `staging_manifest` (list): `[]` until file init
            - `progress` (dict): `{}`
            - `result` (null)
            - `task_id` (null)
            - `cancelled_at` / `completed_at` (null)
            - `created` / `updated` (str): ISO-8601 timestamps

        Raises:
            BadRequest: When `api_version` is present and not 2, or metadata
                is invalid.
        """
        api_version = data.get("api_version", 2)
        if api_version != 2:
            raise BadRequest(
                "JSON create only supports api_version 2 (or omit api_version). "
                "Use multipart/form-data for the legacy sync import."
            )

        metadata = data.get("metadata")
        if metadata is None:
            metadata = []
        if not isinstance(metadata, list):
            raise BadRequest(
                "Invalid metadata. 'metadata' must be an array of metadata objects."
            )

        options = {key: value for key, value in data.items() if key != "metadata"}
        options["api_version"] = 2
        created = self.service.create_import(
            identity=g.identity,
            community_id=community_id,
            records_metadata=metadata,
            options=options,
        )
        return jsonify(created), 202

    def _import_records_legacy(self, community_id: str, data: dict):
        """Legacy multipart sync import (unchanged contract).

        Expects `community_id` as the community UUID or slug from the URL.

        Expects a multipart/form-data request with:

        - files (`ImmutableMultiDict[str, FileStorage]`): The files to import.
          Each key should be `"files"`. Obtain the list via `.getlist("files")`
          on the multi-dict (indexing `"files"` returns only the first value).
        - metadata: JSON array of metadata objects for the records.
        - review_required (bool): Whether to require review of the records.
        - strict_validation (bool): Whether to strictly validate the records.
        - all_or_none (bool): Whether to import all records or none.
        - notify_record_owners (bool): Whether to email identified record owners.
        - no_updates (bool): When True, refuse to update existing matched records
          whose metadata differs. Default False (updates allowed).
        - id_scheme (str): Primary source identifier scheme for dedupe.
        - alternate_id_scheme (str): Optional secondary source identifier scheme.

        Args:
            community_id: Community UUID or slug from the URL.
            data: Parsed multipart payload (`form` + `files`).

        Returns:
            Sync import response with status 201/202/207/400.

        Raises:
            BadRequest: When metadata or boolean form fields are invalid.
        """
        file_data = data["files"]
        app.logger.debug(f"in resource file_data type: {file_data}")
        if isinstance(file_data, ImmutableMultiDict):
            file_data = file_data.getlist("files")

        metadata = json.loads(data["form"].get("metadata"))
        if not isinstance(metadata, list):
            raise BadRequest(
                "Invalid metadata. 'Metadata' must be an array of metadata objects. "
                "Did you submit a single metadata object not enclosed in an array?"
            )

        id_scheme = data["form"].get("id_scheme", "import-recid")
        alternate_id_scheme = data["form"].get("alternate_id_scheme", "")
        review_required = bool_from_string(data["form"].get("review_required", True))
        strict_validation = bool_from_string(
            data["form"].get("strict_validation", True)
        )
        all_or_none = bool_from_string(data["form"].get("all_or_none", True))
        notify_record_owners = bool_from_string(
            data["form"].get("notify_record_owners", False)
        )
        no_updates = bool_from_string(data["form"].get("no_updates", False))

        processed_files = []
        for file in file_data:
            if is_zip_file(file):
                app.logger.debug(f"Extracting zip archive: {file.filename}")
                extracted_files = extract_zip_files(file)
                processed_files.extend(extracted_files)
                app.logger.debug(
                    f"Extracted {len(extracted_files)} files from zip archive"
                )
            else:
                processed_files.append(
                    FileData(
                        filename=file.filename,
                        content_type=file.content_type,
                        mimetype=file.mimetype,
                        mimetype_params=file.mimetype_params,
                        stream=file.stream,
                    )
                )

        file_data = processed_files

        import_result = self.service.import_records(
            identity=g.identity,
            file_data=file_data,
            metadata=metadata,
            id_scheme=id_scheme,
            alternate_id_scheme=alternate_id_scheme,
            community_id=community_id,
            review_required=review_required,
            strict_validation=strict_validation,
            all_or_none=all_or_none,
            no_updates=no_updates,
            notify_record_owners=notify_record_owners,
        )
        app.logger.debug(f"in resource import_result: {import_result.get('status')}")
        status = import_result.get("status")
        if status == "accepted":
            return jsonify(import_result), 202
        if status == "success":
            return Response(
                json.dumps(import_result, sort_keys=False), mimetype="application/json"
            ), 201
        if status in ("multi_status", "partial_success") and not all_or_none:
            return jsonify(import_result), 207
        return jsonify(import_result), 400

    @request_parsed_view_args
    @require_api_auth()
    @require_oauth_scopes("records:import")
    def get_current_import(self):
        """Return the active import for a community, else the latest terminal.

        Returns:
            JSON import payload with status 200.
        """
        community_id = resource_requestctx.view_args.get("community")
        payload = self.service.get_current_import(g.identity, community_id)
        return jsonify(payload), 200

    @request_parsed_view_args
    @require_api_auth()
    @require_oauth_scopes("records:import")
    def list_imports(self):
        """List imports for a community (newest first).

        Returns:
            JSON `{imports, total}` with status 200.
        """
        community_id = resource_requestctx.view_args.get("community")
        payload = self.service.list_imports(g.identity, community_id)
        return jsonify(payload), 200

    @request_parsed_view_args
    @require_api_auth()
    @require_oauth_scopes("records:import")
    def get_import(self):
        """Return one import by id for a community.

        Returns:
            JSON import payload with status 200.
        """
        community_id = resource_requestctx.view_args.get("community")
        import_id = resource_requestctx.view_args.get("import_id")
        payload = self.service.get_import(g.identity, community_id, import_id)
        return jsonify(payload), 200

    @request_parsed_view_args
    @require_api_auth()
    @require_oauth_scopes("records:import")
    def cancel_import(self):
        """Cancel an active import by id.

        Returns:
            JSON cancelled import payload with status 200.
        """
        community_id = resource_requestctx.view_args.get("community")
        import_id = resource_requestctx.view_args.get("import_id")
        payload = self.service.cancel_import(g.identity, community_id, import_id)
        return jsonify(payload), 200

    @request_parsed_view_args
    @require_api_auth()
    @require_oauth_scopes("records:import")
    def cancel_current_import(self):
        """Cancel the active import for a community.

        Returns:
            JSON cancelled import payload with status 200.
        """
        community_id = resource_requestctx.view_args.get("community")
        payload = self.service.cancel_current_import(g.identity, community_id)
        return jsonify(payload), 200


def create_api_blueprint(app):
    """Register blueprint on api app.

    Returns:
        Description of the return value.
    """
    ext = app.extensions["invenio-record-importer-kcworks"]
    blueprint = ext.resource.as_blueprint()

    @blueprint.before_request
    def _raise_import_max_content_length():  # noqa: ANN202
        """Allow large multipart bodies on import routes only."""
        from flask import g as flask_g

        flask_g._record_importer_prev_max_content = app.config.get("MAX_CONTENT_LENGTH")
        app.config["MAX_CONTENT_LENGTH"] = app.config.get(
            "RECORD_IMPORTER_MAX_CONTENT_LENGTH",
            app.config.get("FILES_REST_DEFAULT_MAX_FILE_SIZE", (10**10) * 50),
        )

    @blueprint.after_request
    def _restore_max_content_length(response):  # noqa: ANN001, ANN202
        from flask import g as flask_g

        if hasattr(flask_g, "_record_importer_prev_max_content"):
            app.config["MAX_CONTENT_LENGTH"] = (
                flask_g._record_importer_prev_max_content
            )
        return response

    return blueprint
