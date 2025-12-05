#
# This file is part of the invenio_record_importer_kcworks package.
# Copyright (C) 2024, MESH Research.
#
# invenio_record_importer_kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

import json
from pathlib import Path

import jsonlines
from flask import current_app as app


class SerializationService:
    """Serialization service."""

    # def __init__(self, record_cls, schema):
    #     """Initialize the service."""
    #     self.record_cls = record_cls
    #     self.schema = schema

    # def serialize(self, record):
    #     """Serialize a record."""
    #     return self.schema.dump(record)

    # def dump(self, record):
    #     """Dump a record."""
    #     return self.serialize(record)

    serialized_id_fetchers = {
        "doi": "pids.doi.identifier",
        "hclegacy-pid": lambda x: x["metadata"]["identifiers"][0][
            "identifier"
        ],
    }

    raw_id_fetchers = {
        "doi": "deposit_doi",
        "hclegacy-pid": "id",
    }

    @staticmethod
    def _get_by_dot_string(obj, dot_string):
        """Get value from object by dot string."""
        for key in dot_string.split("."):
            obj = obj.get(key)
        return obj

    @classmethod
    def read_serialized(
        cls,
        identifiers: list[str] = [],
        indices: list[int] = [],
        id_scheme: str = "doi",
        field_path: str = "",
    ) -> list[dict]:
        """Read serialized data.

        When indices are provided they are treated as 0-based indices,
        not 1-based line numbers.

        Returns:
            list[dict]: List of serialized json records as python dictionaries.
        """
        file_path = Path(app.config["RECORD_IMPORTER_SERIALIZED_PATH"])

        serialized_recs = []

        try:
            with jsonlines.open(file_path) as reader:
                iterlist = list(iter(reader))
                if identifiers:
                    id_fetch_path = cls.serialized_id_fetchers[id_scheme]
                    for i in identifiers:
                        if isinstance(id_fetch_path, str):
                            record_val = [
                                r
                                for r in iterlist
                                if cls._get_by_dot_string(r, id_fetch_path)
                                == i
                            ][0]
                        else:
                            # id_fetch_path is a callable when not a string
                            assert callable(id_fetch_path)
                            record_val = [
                                r for r in iterlist if id_fetch_path(r) == i
                            ][0]
                        if field_path:
                            record_val = cls._get_by_dot_string(
                                record_val, field_path
                            )
                        serialized_recs.append(
                            {
                                "id": i,
                                "record": record_val,
                            }
                        )
                elif indices:
                    for n in indices:
                        record_val = iterlist[int(n)]
                        if field_path:
                            record_val = cls._get_by_dot_string(
                                record_val, field_path
                            )
                        serialized_recs.append({"id": n, "record": record_val})
        except IndexError as e:
            raise e

        return serialized_recs

    @classmethod
    def dump_serialized(
        cls,
        identifiers: list[str] = [],
        indices: list[int] = [],
        id_scheme: str = "doi",
        field_path: str = "",
    ) -> str:
        """Dump serialized data."""
        return json.dumps(
            cls.read_serialized(
                identifiers=identifiers,
                indices=indices,
                id_scheme=id_scheme,
                field_path=field_path,
            )
        )

    @classmethod
    def read_raw(
        cls,
        identifiers: list[str] = [],
        indices: list[int] = [],
        id_scheme: str = "doi",
        field_path: str = "",
    ) -> list[dict]:
        """Read raw data."""
        file_path = Path(
            app.config["RECORD_IMPORTER_DATA_DIR"],
            "records-for-import.json",
        )

        print(identifiers, indices, id_scheme, field_path)

        raw_records = []

        with open(file_path) as file:
            data = json.load(file)

            if identifiers:
                id_fetch_path = cls.raw_id_fetchers[id_scheme]
                for i in identifiers:
                    if isinstance(id_fetch_path, str):
                        record_val = [
                            d
                            for d in data
                            if i in cls._get_by_dot_string(d, id_fetch_path)
                        ][0]
                    else:
                        # id_fetch_path is a callable when not a string
                        assert callable(id_fetch_path)
                        record_val = [
                            d for d in data if id_fetch_path(d) == i
                        ][0]
                    if field_path:
                        record_val = cls._get_by_dot_string(
                            record_val, field_path
                        )
                    raw_records.append(
                        {
                            "id": i,
                            "record": record_val,
                        }
                    )
            elif indices:
                for n in indices:
                    record_val = data[int(n)]
                    if field_path:
                        record_val = cls._get_by_dot_string(
                            record_val, field_path
                        )
                    raw_records.append({"id": n, "record": record_val})

        return raw_records

    @classmethod
    def dump_raw(
        cls,
        identifiers: list[str] = [],
        indices: list[int] = [],
        id_scheme: str = "doi",
        field_path: str = "",
    ):
        """Dump raw data."""
        return json.dumps(
            cls.read_raw(
                identifiers=identifiers,
                indices=indices,
                id_scheme=id_scheme,
                field_path=field_path,
            )
        )
