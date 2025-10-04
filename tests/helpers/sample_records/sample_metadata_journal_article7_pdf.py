# Part of the Invenio-Stats-Dashboard extension for InvenioRDM
# Copyright (C) 2025 Mesh Research
#
# Invenio-Stats-Dashboard is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Sample metadata for a journal article 7 with PDF file for testing purposes."""

sample_metadata_journal_article7_pdf = {
    "id": "r4w2d-5tg11",
    "created": "2025-05-30T02:37:21.721607+00:00",
    "metadata": {
        "resource_type": {
            "id": "textDocument-bookSection",
            "title": {"de": "Buchabteilung", "en": "Book section"},
        },
        "creators": [
            {
                "person_or_org": {
                    "type": "personal",
                    "name": "Stout, Andrew C.",
                    "given_name": "Andrew C.",
                    "family_name": "Stout",
                    "identifiers": [
                        {"identifier": "0000-0002-7489-2684", "scheme": "orcid"},
                    ],
                },
                "role": {"id": "author", "title": {"en": "Author"}},
                "affiliations": [{"name": "University of Missouri - St. Louis"}],
            }
        ],
        "title": (
            "The Acts of Unity: The Eucharistic Theology of Charles Williams' Arthurian Poetry"  # noqa: E501
        ),
        "publisher": "Apocryphile Press",
        "publication_date": "2017",
        "subjects": [
            {
                "id": "http://id.worldcat.org/fast/973589",
                "subject": "Inklings (Group of writers)",
                "scheme": "FAST-topical",
            }
        ],
    },
    "custom_fields": {
        "journal:journal": {"pages": "473-492"},
        "imprint:imprint": {
            "title": (
                "The Inklings and King Arthur: J. R. R. Tolkien, Charles Williams, C. S. Lewis, and Owen Barfield on the Matter of Britain"  # noqa: E501
            ),
            "isbn": "9781944769895",
            "pages": "20",
            "place": "Berkeley, CA",
        },
    },
    "access": {
        "record": "public",
        "files": "public",
        "embargo": {"active": False, "reason": None},
        "status": "open",
    },
    "files": {
        "enabled": True,
        "order": [],
        "count": 1,
        "total_bytes": 58659795,
        "entries": {
            "The Acts of Unity.pdf": {
                "ext": "pdf",
                "size": 58659795,
                "mimetype": "application/pdf",
                "key": "The Acts of Unity.pdf",
            },
        },
    },
}
