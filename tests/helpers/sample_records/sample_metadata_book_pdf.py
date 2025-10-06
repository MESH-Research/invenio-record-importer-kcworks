# Part of the Invenio-Stats-Dashboard extension for InvenioRDM
# Copyright (C) 2025 Mesh Research
#
# Invenio-Stats-Dashboard is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Sample metadata for a book with PDF file for testing purposes."""

sample_metadata_book_pdf = {
    "created": "2020-01-30T16:46:54Z",
    "custom_fields": {
        "imprint:imprint": {"isbn": "978-84-200-1103-5"},
    },
    "files": {
        "default_preview": (
            "tratamiento-de-los-residuos-de-la-"
            "industria-del-procesado-de-alimentos.pdf"
        ),
        "enabled": True,
        "entries": {
            "tratamiento-de-los-residuos-de-la-industria-del-procesado-de-alimentos.pdf": {  # noqa
                "key": (
                    "tratamiento-de-los-residuos-de-la-industria-del-"
                    "procesado-de-alimentos.pdf"
                ),
                "mimetype": "application/pdf",
                "size": "17181",
            }
        },
    },
    "metadata": {
        "additional_descriptions": [
            {
                "description": (
                    "Wang, Lawrence K, Hung, Yung-Tse, Lo, Howard H, "
                    "Yapijakis, Constantine  and  Ribas, Alberto "
                    "lbarz (2008).  TRATAMIENTO de los RESIDUOS de la "
                    "INDUSTRIA del PROCESADO de ALIMENTOS  "
                    "(Spanish).  Waste Treatment in the Food Processing "
                    "Industry.   Editorial ACRIBIA, S. A.,, Apartado "
                    "466, 50080, Zaragoza, Espana. 398 pages. ISBN  "
                    "978-84-200-1103-5  ---------------ABSTRACT:  This "
                    "book "
                    "emphasizes in-depth presentation of "
                    "environmental pollution sources, waste "
                    "characteristics, control technologies, "
                    "management strategies, facility "
                    "innovations, process alternatives, "
                    "costs, case histories, effluent "
                    "standards, and future trends for the food "
                    "industry, and in-depth presentation of methodologies,"
                    " "
                    "technologies, alternatives, regional effects, "
                    "and global effects of important pollution control "
                    "practice that may be applied to the industry.  "
                    "Important waste treatment topics covered in this "
                    "book include: dairies, seafood processing plants, "
                    "olive oil manufacturing factories, potato "
                    "processing installations, soft drink "
                    "production plants, bakeries and various other food "
                    "processing facilities."
                ),
                "type": {"id": "other", "title": {"en": "Other"}},
            }
        ],
        "creators": [
            {
                "person_or_org": {
                    "family_name": "Hung",
                    "given_name": "Yung-Tse",
                    "name": "Hung, Yung-Tse",
                    "type": "personal",
                },
                "role": {"id": "editor", "title": {"en": "Editor"}},
            },
            {
                "person_or_org": {
                    "family_name": "Lo",
                    "given_name": "Howard H",
                    "name": "Lo, Howard H",
                    "type": "personal",
                },
                "role": {"id": "editor", "title": {"en": "Editor"}},
            },
            {
                "person_or_org": {
                    "family_name": "Ribas",
                    "given_name": "Alberto lbarz",
                    "name": "Ribas, Alberto lbarz",
                    "type": "personal",
                },
                "role": {
                    "id": "translator",
                    "title": {"en": "Translator"},
                },
            },
            {
                "person_or_org": {
                    "family_name": "Wang",
                    "given_name": "Lawrence K",
                    "name": "Wang, Lawrence K",
                    "type": "personal",
                },
                "role": {"id": "editor", "title": {"en": "Editor"}},
            },
            {
                "person_or_org": {
                    "family_name": "Yapijakis",
                    "given_name": "Constantine",
                    "name": "Yapijakis, Constantine",
                    "type": "personal",
                },
                "role": {"id": "editor", "title": {"en": "Editor"}},
            },
        ],
        "description": (
            "Wang, Lawrence K, Hung, Yung-Tse, Lo, Howard H, "
            "Yapijakis, Constantine  and  Ribas, Alberto "
            "lbarz (2008).  TRATAMIENTO de los RESIDUOS de "
            "la INDUSTRIA del PROCESADO de ALIMENTOS  "
            "(Spanish).  Waste Treatment in the Food "
            "Processing Industry.   Editorial ACRIBIA, S. "
            "A.,, Apartado 466, 50080, Zaragoza, Espana. 398 "
            "pages. ISBN  978-84-200-1103-5  "
            "---------------ABSTRACT:  This book emphasizes "
            "in-depth presentation of environmental "
            "pollution sources, waste characteristics, "
            "control technologies, management strategies, "
            "facility innovations, process alternatives, "
            "costs, case histories, effluent standards, and "
            "future trends for the food industry, and "
            "in-depth presentation of methodologies, "
            "technologies, alternatives, regional effects, "
            "and global effects of important pollution "
            "control practice that may be applied to the "
            "industry.  Important waste treatment topics "
            "covered in this book include: dairies, seafood "
            "processing plants, olive oil manufacturing "
            "factories, potato processing installations, "
            "soft drink production plants, bakeries and "
            "various other food processing facilities."
        ),
        "identifiers": [],
        "languages": [{"id": "spa", "title": {"en": "Spanish"}}],
        "publication_date": "2008",
        "resource_type": {"id": "textDocument-book"},
        "publisher": (
            "Editorial ACRIBIA, S. A., Apartado 466, 50080, " "Zaragoza, Espana."
        ),
        "rights": [
            {
                "description": {"en": "All Rights Reserved"},
                "id": "arr",
                "icon": "copyright",
                "props": {"url": "https://arr.org/licenses/all-rights-reserved"},
                "title": {"en": "All Rights Reserved"},
            }
        ],
        "subjects": [
            {
                "id": "http://id.worldcat.org/fast/1108387",
                "subject": "Science--Study and teaching",
                "scheme": "FAST-topical",
            },
            {
                "id": "http://id.worldcat.org/fast/1145221",
                "subject": "Technology--Study and teaching",
                "scheme": "FAST-topical",
            },
        ],
        "title": (
            "TRATAMIENTO de los RESIDUOS de la INDUSTRIA del " "PROCESADO de ALIMENTOS"
        ),
    },
}
