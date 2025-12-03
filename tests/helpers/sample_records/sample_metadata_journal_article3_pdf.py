"""Sample record metadata for testing."""

# Part of the Invenio-Stats-Dashboard extension for InvenioRDM
# Copyright (C) 2025 Mesh Research
#
# Invenio-Stats-Dashboard is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Sample metadata for a journal article 3 with PDF file for testing purposes."""

sample_metadata_journal_article3_pdf = {
    "id": "5ce94-3yt37",
    "created": "2025-06-12T18:43:57.051364+00:00",
    "updated": "2025-06-12T18:43:57.174272+00:00",
    "metadata": {
        "resource_type": {
            "id": "textDocument-journalArticle",
            "title": {"de": "Zeitschriftenartikel", "en": "Journal article"},
        },
        "creators": [
            {
                "person_or_org": {
                    "type": "personal",
                    "name": "Eleuterio, David",
                    "given_name": "David",
                    "family_name": "Eleuterio",
                    "identifiers": [
                        {"identifier": "david-alma@hotmail.com", "scheme": "email"}
                    ],
                },
                "role": {"id": "author", "title": {"en": "Author"}},
            }
        ],
        "title": "Algumas considerações acerca da arquitetura civil portucalense",
        "publisher": "ARIC –FACULDADE DAMAS DA INSTRUÇÃO CRISTÃ",
        "publication_date": "2012-06-12",
        "languages": [{"id": "por", "title": {"en": "Portuguese"}}],
        "description": (
            "The following  article  will  inciteon  the  constitution  of  the portuensesocietysince  the restoration  of  the  portucalense  bishop  until  the  end  of  the  time  of  king  D.  João  II,  inquiring the edifying methodologies in use and the form how these ones characterize the architectural pathway  from  the  medieval  borough.  In  order  to  elucidate  the  presentation,  it  will  focus  in some  specific  cases,  which  will  be  followed  by  an  analysis  of  the  executed  archeological interventions, as well as the current politics of requalification optimized at Morro da Sé.    Keywords:civil architecture, territory planning, heritage, portuense society."  # noqa: E501
        ),
    },
    "custom_fields": {
        "journal:journal": {
            "title": "ARCHITECTON -REVISTA DE ARQUITETURA E URBANISMO",
            "issue": "1",
            "volume": "2",
            "pages": "46 - 61",
            "issn": "2236-6849",
        },
        "imprint:imprint": {"place": "Recife, Brazil"},
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
        "total_bytes": 1768474,
        "entries": {
            "1305.pdf": {
                "ext": "pdf",
                "size": 1768474,
                "mimetype": "application/pdf",
                "key": "1305.pdf",
            }
        },
    },
}
# noqa: E501
