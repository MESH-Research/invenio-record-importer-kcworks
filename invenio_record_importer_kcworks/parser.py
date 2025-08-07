from functools import wraps

from flask import current_app as app
from flask import request
from flask_resources.config import resolve_from_conf
from flask_resources.context import resource_requestctx
from flask_resources.deserializers import JSONDeserializer
from flask_resources.errors import InvalidContentType
from flask_resources.parsers.body import RequestBodyParser


def request_body_parser(
    parsers={"application/json": RequestBodyParser(deserializer=JSONDeserializer())},
    default_content_type="application/json",
):
    """Create decorator for parsing the request body.

    # NOTE: Replaces flask_resources.parsers.decorators:request_body_parser
    # which couldn't handle multipart/form-data
    # FIXME: Submit a PR to fix this, or perhaps switch to a form handling library?

    Both decorator parameters can be resolved from the resource configuration.

    :param parsers: A mapping of content types to parsers.
    :param default_content_type_name: The default content type used to select
        a parser if no content type was provided.
    """

    def decorator(f):
        @wraps(f)
        def inner(self, *args, **kwargs):
            # Get the possible parsers
            body_parsers = resolve_from_conf(parsers, self.config)

            # Get the request body content type
            content_type = request.content_type or resolve_from_conf(
                default_content_type, self.config
            )
            if "multipart/form-data" in content_type:
                content_type = content_type.split(";")[0]

            # Get the parser
            parser = body_parsers.get(content_type)
            if parser is None:
                raise InvalidContentType(allowed_mimetypes=body_parsers.keys())

            # Parse the request body.
            resource_requestctx.data = parser.parse()

            return f(self, *args, **kwargs)

        return inner

    return decorator


class RequestMultipartParser:
    """Parse the request multipart data."""

    def parse(self):
        """Parse the request multipart data."""
        app.logger.debug(f"form type: {type(request.form)}")
        app.logger.debug(f"Request form: {request.form}")
        app.logger.debug(f"files type: {type(request.files)}")
        app.logger.debug(f"Request files: {request.files}")
        return {
            "form": request.form,
            "files": request.files,
        }
