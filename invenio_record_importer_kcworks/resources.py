from flask import g, jsonify, current_app as app
from flask_resources import Resource, ResourceConfig
from flask_resources.config import from_conf
from flask_resources.context import resource_requestctx
from flask_resources.parsers.decorators import request_parser
from flask_resources.responses import ResponseHandler
from flask_resources.resources import route
from flask_resources.serializers.json import JSONSerializer
from invenio_records_resources.services.errors import PermissionDeniedError
import json
import marshmallow as ma
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.exceptions import (
    Forbidden,
    MethodNotAllowed,
    NotFound,
    BadRequest,
    UnprocessableEntity,
)

from .parser import RequestMultipartParser, request_body_parser
from .types import FileData


def bool_from_string(value: str) -> bool:
    """Convert a string to a boolean.

    If the value is already a boolean, just return it. If the value is not a
    standard string representation of a boolean, raise a BadRequest exception.
    """
    if value in ["true", "True", "TRUE", "1", True]:
        return True
    elif value in ["false", "False", "FALSE", "0", False]:
        return False
    else:
        raise BadRequest(f"Invalid boolean value: {value}")


# Decorators

request_form_data = request_body_parser(
    parsers=from_conf("request_body_parsers"),
    default_content_type=str(from_conf("default_content_type")),
)

request_parsed_view_args = request_parser(
    {
        "community": ma.fields.String(),
    },
    location="view_args",
)


class RecordImporterResourceConfig(ResourceConfig):
    blueprint_name = "record_importer_kcworks"

    url_prefix = "/import"

    # Error handlers are registered on the blueprint by flask_resources
    # using the ``Blueprint.register_error_handler()`` method.
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
        BadRequest: lambda e: (
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
    request_body_parsers = {"multipart/form-data": RequestMultipartParser()}

    # Response handling
    default_accept_mimetype = "application/json"
    response_handlers = {"application/json": ResponseHandler(JSONSerializer())}


class RecordImporterResource(Resource):

    def __init__(self, config, service):
        super().__init__(config)
        self.service = service

    def create_url_rules(self):
        """Create the URL rules for the record resource.

        Registered by flask_resources on the blueprint using the
        ``Blueprint.add_url_rule()`` method.
        """
        return [
            route("POST", "/<community>", self.import_records),
        ]

    @request_parsed_view_args
    @request_form_data
    def import_records(self):
        """Import records.

        Expects one view argument (url path parameter) for the community ID. This
        can be either the community UUID or the community url slug.

        Expects a multipart/form-data request with the following fields in the
        form data:

        - community(str): The community ID.
        - files(ImmutableMultiDict[str, FileStorage]): The files to import. Each
          key in the dictionary should be identical, "files". Each value is a
          werkzeug.datastructures.FileStorage object. *Note: the list of values
          must be obtained by calling .getlist("files") on the ImmutableMultiDict.
          Requesting the key "files" will return only the first value in the
          dictionary.
        - metadata(dict): The metadata to attach to the records.
        - review_required(bool): Whether to require review of the records.
        - strict_validation(bool): Whether to strictly validate the records.
        - all_or_none(bool): Whether to import all records or none.
        """
        community_id = resource_requestctx.view_args.get("community")
        file_data = resource_requestctx.data["files"]
        app.logger.debug(f"in resource file_data type: {file_data}")
        if isinstance(file_data, ImmutableMultiDict):
            file_data = file_data.getlist("files")

        metadata = json.loads(resource_requestctx.data["form"].get("metadata"))
        app.logger.debug(f"metadata type: {type(metadata)}")
        if not isinstance(metadata, list):
            raise BadRequest(
                "Invalid metadata. 'Metadata' must be an array of metadata objects. "
                "Did you submit a single metadata object not enclosed in an array?"
            )

        id_scheme = resource_requestctx.data["form"].get("id_scheme", "neh-recid")
        alternate_id_scheme = resource_requestctx.data["form"].get(
            "alternate_id_scheme", ""
        )
        review_required = bool_from_string(
            resource_requestctx.data["form"].get("review_required", True)
        )
        strict_validation = bool_from_string(
            resource_requestctx.data["form"].get("strict_validation", True)
        )
        all_or_none = bool_from_string(
            resource_requestctx.data["form"].get("all_or_none", True)
        )

        file_data = [
            FileData(
                filename=file.filename,
                content_type=file.content_type,
                mimetype=file.mimetype,
                mimetype_params=file.mimetype_params,
                stream=file.stream,
            )
            for file in file_data
        ]
        #  TODO: use the Flask secure_filename function to sanitize the file name

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
        )
        app.logger.debug(f"in resource import_result: {import_result.get('status')}")
        if import_result.get("status") == "success":
            return jsonify(import_result), 201
        elif import_result.get("status") == "multi_status" and not all_or_none:
            return jsonify(import_result), 207
        else:
            return jsonify(import_result), 400


def create_api_blueprint(app):
    """Register blueprint on api app."""

    ext = app.extensions["invenio-record-importer-kcworks"]
    blueprint = ext.resource.as_blueprint()

    return blueprint
