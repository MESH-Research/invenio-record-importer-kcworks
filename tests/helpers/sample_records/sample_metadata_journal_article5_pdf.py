# Part of the Invenio-Stats-Dashboard extension for InvenioRDM
# Copyright (C) 2025 Mesh Research
#
# Invenio-Stats-Dashboard is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Sample metadata for a journal article 5 with PDF file for testing purposes."""

sample_metadata_journal_article5_pdf = {
    "id": "0dtmf-ph235",
    "created": "2025-06-03T17:35:55.817258+00:00",
    "metadata": {
        "resource_type": {
            "id": "textDocument-book",
            "title": {"de": "Buch", "en": "Book"},
        },
        "creators": [
            {
                "person_or_org": {
                    "type": "personal",
                    "name": "Dollinger, Stefan",
                    "given_name": "Stefan",
                    "family_name": "Dollinger",
                    "identifiers": [],
                },
                "role": {"id": "author", "title": {"en": "Author"}},
                "affiliations": [{"name": "University Of British Columbia"}],
            },
            {
                "person_or_org": {
                    "type": "personal",
                    "name": "Fee, Margery",
                    "given_name": "Margery",
                    "family_name": "Fee",
                },
                "role": {"id": "author", "title": {"en": "Author"}},
                "affiliations": [
                    {"id": "03rmrcq20", "name": "University of British Columbia"}
                ],
            },
        ],
        "title": (
            "Dictionary of Canadianisms on Historical Principles, Third Edition (www.dchp.ca/dchp3)"  # noqa: E501
        ),
        "additional_titles": [
            {
                "title": "DCHP-3",
                "type": {
                    "id": "alternative-title",
                    "title": {
                        "de": "Alternativer Titel",
                        "en": "Alternative title",
                    },
                },
            }
        ],
        "publisher": "UBC",
        "publication_date": "2025-06-03",
        "subjects": [
            {
                "id": "http://id.worldcat.org/fast/911979",
                "subject": "English language--Written English--History",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/911660",
                "subject": "English language--Spoken English--Research",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/845111",
                "subject": "Canadian literature",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/845142",
                "subject": "Canadian literature--Periodicals",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/845184",
                "subject": "Canadian prose literature",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/1424786",
                "subject": "Canadian literature--Bibliography",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/934875",
                "subject": "French-Canadian literature",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/817954",
                "subject": "Arts, Canadian",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/821870",
                "subject": "Authors, Canadian",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/845170",
                "subject": "Canadian periodicals",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/911328",
                "subject": "English language--Lexicography--History",
                "scheme": "FAST-topical",
            },
        ],
        "description": (
            'This is the third edition of the 1967 A Dictionary of Canadianisms on Historical Principles (DCHP-1). DCHP-3 integrates the legacy data of DCHP-1 (1967) and the updated data of DCHP-2 (2017) with new content to form DCHP-3. There are 136 new and updated entries in this edition for a new total of 12,045 headwords with 14,586 meanings.\n\nDCHP-3 lists, as did its predecessors, Canadianisms. A Canadianism is defined as "a word, expression, or meaning which is native to Canada or which is distinctively characteristic of Canadian usage though not necessarily exclusive to Canada." (Walter S. Avis in DCHP-1, page xiii; see DCHP-1 Online)\n\nThis work should be cited as:\n\nDollinger, Stefan and Margery Fee (eds). 2025. DCHP-3: The Dictionary of Canadianisms on Historical Principles, Third Edition. Vancouver, BC: University of British Columbia, www.dchp.ca/dchp3.'  # noqa: E501
        ),
    },
    "custom_fields": {
        "imprint:imprint": {
            "pages": "Online publication",
            "place": "Vancouver, BC",
        },
    },
    "access": {
        "record": "public",
        "files": "restricted",
        "embargo": {"active": False, "reason": None},
        "status": "metadata-only",
    },
    "files": {
        "enabled": False,
        "order": [],
        "count": 0,
        "total_bytes": 0,
        "entries": {},
    },
}
