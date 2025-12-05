from io import BufferedReader
from pprint import pformat
from tempfile import SpooledTemporaryFile
from typing import Any, TypedDict

from pydantic import BaseModel, field_validator


class FileUploadResult(TypedDict):
    """Type definition for file upload result structure."""
    status: str
    messages: list[str]


class FileData(BaseModel):
    """A class to represent the file data for a record to be imported.
    """

    filename: str
    content_type: str
    mimetype: str
    mimetype_params: dict
    stream: SpooledTemporaryFile | BufferedReader

    class Config:
        arbitrary_types_allowed = True

    @field_validator("stream")
    @classmethod
    def validate_temp_file(
        cls, v: Any
    ) -> SpooledTemporaryFile | BufferedReader | None:
        if v is not None and not isinstance(v, (SpooledTemporaryFile, BufferedReader)):
            raise ValueError("Must be a SpooledTemporaryFile or BufferedReader")
        return v


class ImportedRecord(BaseModel):
    """A class to represent a record import result.

    item_index(int): The index of the record in the source metadata list.
    record_id(str): The InvenioRDM record ID of the created record.
    record_url(str): The html URL of the created record.
    files(list[FileData]): The files to be uploaded for the record.
    collection_id(str): The ID of the collection the record belongs to.
    errors(list[str]): The errors that occurred during the import.
    metadata(dict): The metadata of the record.
    """

    item_index: int
    record_id: str
    source_id: str
    record_url: str
    files: dict[str, FileUploadResult]
    collection_id: str
    errors: list[dict]
    metadata: dict


class APIResponsePayload(BaseModel):
    """A class to represent an API endpoint response payload.
    """

    status: str
    data: list[dict] = []
    errors: list[dict] = []
    message: str = ""


class LoaderResult(BaseModel):
    """A class to represent the loader result for one record.
    """

    index: int
    source_id: str = ""
    log_object: dict = {}
    primary_community: dict = {}
    record_created: dict = {}
    existing_record: dict = {}
    uploaded_files: dict[str, FileUploadResult] = {}
    community_review_result: dict = {}
    assigned_owners: dict = {}
    added_to_collections: list = []
    status: str = ""
    errors: list[dict] = []
    submitted: dict = {}

    def __str__(self) -> str:
        """Return a pretty-printed string representation of the LoaderResult."""
        # Create a dictionary with the most important fields
        result_dict = {
            "index": self.index,
            "source_id": self.source_id,
            "status": self.status,
            "record_id": self.record_created.get("record_data", {}).get("id", ""),
            "errors": self.errors,
            "uploaded_files": self.uploaded_files,
            "primary_community": self.primary_community.get("id", ""),
            "assigned_owners": self.assigned_owners,
            "record_created": self.record_created,
            "existing_record": self.existing_record,
            "community_review_result": self.community_review_result,
            "added_to_collections": self.added_to_collections,
            "submitted": self.submitted,
        }

        return pformat(result_dict, indent=2, width=100)
