from typing import Any, Union
from pydantic import BaseModel, field_validator
from tempfile import SpooledTemporaryFile
from io import BufferedReader


class FileData(BaseModel):
    """
    A class to represent the file data for a record to be imported.
    """

    filename: str
    content_type: str
    mimetype: str
    mimetype_params: dict
    stream: Union[SpooledTemporaryFile, BufferedReader]

    class Config:
        arbitrary_types_allowed = True

    @field_validator("stream")
    @classmethod
    def validate_temp_file(
        cls, v: Any
    ) -> Union[SpooledTemporaryFile, BufferedReader, None]:
        if v is not None and not isinstance(v, (SpooledTemporaryFile, BufferedReader)):
            raise ValueError("Must be a SpooledTemporaryFile or BufferedReader")
        return v


class ImportedRecord(BaseModel):
    """
    A class to represent a record import result.

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
    files: dict[str, list[Union[str, list[str]]]]
    collection_id: str
    errors: list[dict]
    metadata: dict


class APIResponsePayload(BaseModel):
    """
    A class to represent an API endpoint response payload.
    """

    status: str
    data: list[dict] = []
    errors: list[dict] = []
    message: str = ""


class LoaderResult(BaseModel):
    """
    A class to represent the loader result for one record.
    """

    index: int
    source_id: str = ""
    log_object: dict = {}
    primary_community: dict = {}
    record_created: dict = {}
    existing_record: dict = {}
    uploaded_files: dict[str, list[Union[str, list[str]]]] = {}
    community_review_result: dict = {}
    assigned_owners: dict = {}
    added_to_collections: list = []
    status: str = ""
    errors: list[dict] = []
    submitted: dict = {}
