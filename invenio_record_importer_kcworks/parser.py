from flask import request


class RequestMultipartParser:
    """Parse the request multipart data."""

    def parse(self):
        """Parse the request multipart data."""
        return {
            "form": request.form,
            "files": request.files,
        }
