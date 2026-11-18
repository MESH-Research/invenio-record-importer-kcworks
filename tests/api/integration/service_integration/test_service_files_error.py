# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for the RecordImportService.import_records with file errors."""

import copy

from tests.helpers.sample_records import (
    sample_metadata_chapter2_pdf,
    sample_metadata_chapter_pdf,
)

from .test_service_metadata_success import ServiceTest


class TestServiceErrorJArticleMissingFile(ServiceTest):
    """Test importing two journal articles with an error in the file."""

    @property
    def metadata_sources(self):  # noqa: D102
        meta1 = copy.deepcopy(sample_metadata_chapter_pdf)
        meta1["metadata"]["title"] = ""
        meta2 = copy.deepcopy(sample_metadata_chapter2_pdf)
        return [meta1, meta2]

    @property
    def files_to_upload(self):
        """Override the default files to upload to remove the first file.

        The first record should fail now, even though the second record
        is the one with the invalid metadata.
        """
        files, file_list, file_streams = super().files_to_upload
        file_streams[0].close()
        file_streams = file_streams[1:]
        files = files[1:]
        # leave the file list the same so that there's a mismatch between
        # the files and the file list (entries)
        return files, file_list, file_streams

    @property
    def expected_errors(self):
        """The first file is missing, second has invalid metadata."""
        return [
            [
                {
                    "file_upload_error": {
                        "file upload failures": {
                            "sample.pdf": {
                                "status": "failed",
                                "messages": [
                                    "File sample.pdf not found in list of files."
                                ],
                            }
                        },
                    },
                },
            ],
        ]
