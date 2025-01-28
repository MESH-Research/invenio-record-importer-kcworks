from flask import jsonify, request
from flask_resources import Resource, ResourceConfig
from flask_resources.config import from_conf
from flask_resources.context import resource_requestctx
from flask_resources.parsers.body import RequestBodyParser
from flask_resources.parsers.decorators import request_parser, request_body_parser
from flask_resources.deserializers.json import JSONDeserializer
from flask_resources.responses import ResponseHandler
from flask_resources.resources import route
from flask_resources.serializers.json import JSONSerializer
import marshmallow as ma
from werkzeug.exceptions import (
    Forbidden,
    MethodNotAllowed,
    NotFound,
    BadRequest,
    UnprocessableEntity,
)

from .parser import RequestMultipartParser

# Decorators
# request_data = request_body_parser(
#     parsers=from_conf("request_body_parsers"),
#     default_content_type=from_conf("default_content_type"),
# )

request_form_data = request_body_parser(
    parsers={"multipart/form-data": RequestBodyParser(JSONDeserializer())},
    default_content_type="multipart/form-data",
)

request_parsed_view_args = request_parser(
    {
        "community": ma.fields.String(),
    },
    location="view_args",
)

# request_parsed_args = request_parser(
#     {
#         "commons_instance": ma.fields.String(),
#         "commons_group_id": ma.fields.String(),
#         "collection": ma.fields.String(),
#         "page": ma.fields.Integer(load_default=1),
#         "size": ma.fields.Integer(
#             validate=ma.validate.Range(min=4, max=1000), load_default=25
#         ),
#         "sort": ma.fields.String(
#             validate=ma.validate.OneOf(
#                 [
#                     "newest",
#                     "oldest",
#                     "updated-desc",
#                     "updated-asc",
#                 ]
#             ),
#             load_default="updated-desc",
#         ),
#         "restore_deleted": ma.fields.Boolean(load_default=False),
#     },
#     location="args",
# )


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
        community_id = resource_requestctx.view_args.get("community")
        file_data = request.files
        review_required = request.form.get("review_required", True)
        strict_validation = request.form.get("strict_validation", True)
        all_or_none = request.form.get("all_or_none", True)

        ## TODO: use the Falsk secure_filename function to sanitize the file name

        import_result = self.service.import_records(
            file_data,
            community_id,
            review_required=review_required,
            strict_validation=strict_validation,
            all_or_none=all_or_none,
        )
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
