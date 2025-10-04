# Part of the Invenio-Stats-Dashboard extension for InvenioRDM
# Copyright (C) 2025 Mesh Research
#
# Invenio-Stats-Dashboard is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Sample metadata for a journal article with PDF file for testing purposes."""

sample_metadata_journal_article_pdf = {
    "access": {
        "files": "public",
        "record": "public",
        "status": "open",
    },
    "custom_fields": {
        "journal:journal": {
            "title": "Philological Encounters",
            "issue": "3",
            "pages": "308-352",
            "issn": "2451-9189",
            "volume": "5",
        },
    },
    "files": {
        "enabled": True,
        "entries": {
            "24519197_005_03-04_s004_text.pdf": {
                "key": "24519197_005_03-04_s004_text.pdf",
                "mimetype": "application/pdf",
                "size": "17181",
            }
        },
    },
    "metadata": {
        "creators": [
            {
                "affiliations": [{"name": "University of Southern California"}],
                "person_or_org": {
                    "family_name": "Roberts",
                    "given_name": "Alexandre",
                    "name": "Roberts, Alexandre",
                    "type": "personal",
                },
                "role": {"id": "author", "title": {"en": "Author"}},
            }
        ],
        "dates": [
            {
                "date": "2020-10-13",
                "description": "Publication date",
                "type": {
                    "id": "issued",
                    "title": {"en": "Issued"},
                },
            }
        ],
        "description": (
            "This article examines an Arabic mathematical "
            "manuscript at Columbia University’s Rare Book "
            "and Manuscript Library (or. 45), focusing on a "
            "previously unpublished set of texts: the "
            "treatise on the mathematical method known as "
            "Double False Position, as supplemented by Jābir "
            "ibn Ibrāhīm al-Ṣābī (tenth century?), and the "
            "commentaries by Aḥmad ibn al-Sarī (d. "
            "548/1153–4) and Saʿd al-Dīn Asʿad ibn Saʿīd "
            "al-Hamadhānī (12th/13th century?), the latter "
            "previously unnoticed. The article sketches the "
            "contents of the manuscript, then offers an "
            "editio princeps, translation, and analysis of "
            "the treatise. It then considers how the Swiss "
            "historian of mathematics Heinrich Suter "
            "(1848–1922) read Jābir’s treatise (as contained "
            "in a different manuscript) before concluding "
            "with my own proposal for how to go about "
            "reading this mathematical text: as a witness of "
            "multiple stages of a complex textual tradition "
            "of teaching, extending, and rethinking "
            "mathematics—that is, we should read it "
            "philologically."
        ),
        "identifiers": [
            {"identifier": "10.1163/24519197-BJA10007", "scheme": "doi"},
            {"identifier": "2451-9197", "scheme": "issn"},
        ],
        "languages": [{"id": "eng", "title": {"en": "English"}}],
        "publication_date": "2020",
        "resource_type": {"id": "textDocument-journalArticle"},
        "rights": [
            {
                "id": "arr",
                "title": {"en": "All Rights Reserved"},
            }
        ],
        "subjects": [
            {
                "id": "http://id.worldcat.org/fast/1108176",
                "subject": "Science",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/958235",
                "subject": "History",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/1012213",
                "subject": "Mathematics--Philosophy",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/1012163",
                "subject": "Mathematics",
                "scheme": "FAST-topical",
            },
        ],
        "publisher": "Brill",
        "title": (
            "Mathematical Philology in the Treatise on Double "
            "False Position in an Arabic Manuscript at Columbia "
            "University"
        ),
    },
    "parent": {
        "access": {
            "owned_by": [
                {
                    "email": "test@example.com",
                },
                {
                    "full_name": "John Doe",
                    "email": "john.doe@example.com",
                    "identifiers": [
                        {"identifier": "0000-0002-1825-0097", "scheme": "orcid"},
                    ],
                },
            ]
        }
    },
    "pids": {
        "doi": {
            "client": "datacite",
            "identifier": "10.17613/xxxj-e936",
            "provider": "datacite",
        }
    },
}
