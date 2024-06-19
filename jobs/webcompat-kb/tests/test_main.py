from datetime import datetime, timezone
from unittest.mock import Mock, patch
from google.cloud.bigquery import Row

import pytest

from webcompat_kb.main import BugzillaToBigQuery
from webcompat_kb.main import extract_int_from_field
from webcompat_kb.main import parse_string_to_json
from webcompat_kb.main import parse_datetime_str
from webcompat_kb.main import RELATION_CONFIG, LINK_FIELDS

SAMPLE_BUGS = [
    {
        "see_also": [
            "https://github.com/webcompat/web-bugs/issues/13503",
            "https://github.com/webcompat/web-bugs/issues/91682",
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1633399",
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1735227",
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1739489",
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1739791",
            "https://github.com/webcompat/web-bugs/issues/109064",
            "https://github.com/mozilla-extensions/webcompat-addon/blob/5b391018e847a1eb30eba4784c86acd1c638ed26/src/injections/js/bug1739489-draftjs-beforeinput.js",  # noqa
            "https://github.com/webcompat/web-bugs/issues/112848",
            "https://github.com/webcompat/web-bugs/issues/117039",
        ],
        "cf_user_story": "url:cmcreg.bancosantander.es/*\r\nurl:new.reddit.com/*\r\nurl:web.whatsapp.com/*\r\nurl:facebook.com/*\r\nurl:twitter.com/*\r\nurl:reddit.com/*\r\nurl:mobilevikings.be/*\r\nurl:book.ersthelfer.tv/*",  # noqa
        "severity": "--",
        "priority": "--",
        "depends_on": [903746],
        "component": "Knowledge Base",
        "product": "Web Compatibility",
        "resolution": "",
        "status": "NEW",
        "blocks": [],
        "id": 1835339,
        "summary": "Missing implementation of textinput event",
        "assigned_to": "test@example.org",
        "creation_time": "2000-07-25T13:50:04Z",
        "keywords": [],
        "url": "",
        "whiteboard": "",
    },
    {
        "component": "Knowledge Base",
        "product": "Web Compatibility",
        "depends_on": [],
        "see_also": [
            "https://github.com/webcompat/web-bugs/issues/100260",
            "https://github.com/webcompat/web-bugs/issues/22829",
            "https://github.com/webcompat/web-bugs/issues/62926",
            "https://github.com/webcompat/web-bugs/issues/66768",
            "https://github.com/webcompat/web-bugs/issues/112423",
            "https://mozilla.github.io/standards-positions/#webusb",
            "https://github.com/webcompat/web-bugs/issues/122436",
            "https://github.com/webcompat/web-bugs/issues/122127",
            "https://github.com/webcompat/web-bugs/issues/120886",
        ],
        "summary": "Sites breaking due to the lack of WebUSB support",
        "id": 1835416,
        "blocks": [],
        "resolution": "",
        "priority": "--",
        "severity": "--",
        "cf_user_story": "url:webminidisc.com/*\r\nurl:app.webadb.com/*\r\nurl:www.numworks.com/*\r\nurl:webadb.github.io/*\r\nurl:www.stemplayer.com/*\r\nurl:wootility.io/*\r\nurl:python.microbit.org/*\r\nurl:flash.android.com/*",  # noqa
        "status": "NEW",
        "assigned_to": "nobody@mozilla.org",
        "creation_time": "2000-07-25T13:50:04Z",
        "keywords": [],
        "url": "",
        "whiteboard": "",
    },
    {
        "component": "Knowledge Base",
        "product": "Web Compatibility",
        "depends_on": [555555],
        "see_also": [
            "https://crbug.com/606208",
            "https://github.com/whatwg/html/issues/1896",
            "https://w3c.github.io/trusted-types/dist/spec/",
            "https://github.com/webcompat/web-bugs/issues/124877",
            "https://github.com/mozilla/standards-positions/issues/20",
            "https://github.com/WebKit/standards-positions/issues/186",
        ],
        "summary": "Test bug",
        "id": 111111,
        "blocks": [222222, 1734557],
        "resolution": "",
        "priority": "--",
        "severity": "--",
        "cf_user_story": "",
        "status": "NEW",
        "assigned_to": "nobody@mozilla.org",
        "creation_time": "2000-07-25T13:50:04Z",
        "keywords": [],
        "url": "",
        "whiteboard": "",
    },
]

SAMPLE_CORE_BUGS = [
    {
        "id": 903746,
        "severity": "--",
        "priority": "--",
        "cf_user_story": "",
        "depends_on": [],
        "status": "UNCONFIRMED",
        "product": "Core",
        "blocks": [1754236, 1835339],
        "component": "DOM: Events",
        "see_also": [
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1739489",
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1739791",
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1735227",
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1633399",
            "https://github.com/webcompat/web-bugs/issues/109064",
            "https://github.com/webcompat/web-bugs/issues/112848",
            "https://github.com/webcompat/web-bugs/issues/117039",
            "https://github.com/w3c/uievents/issues/353",
        ],
        "resolution": "",
        "summary": "Missing textinput event",
        "assigned_to": "nobody@mozilla.org",
    },
    {
        "id": 555555,
        "severity": "--",
        "priority": "--",
        "cf_user_story": "",
        "depends_on": [],
        "status": "UNCONFIRMED",
        "product": "Core",
        "blocks": [],
        "component": "Test",
        "see_also": ["https://mozilla.github.io/standards-positions/#testposition"],
        "resolution": "",
        "summary": "Test Core bug",
        "assigned_to": "nobody@mozilla.org",
    },
]

SAMPLE_BREAKAGE_BUGS = [
    {
        "id": 1734557,
        "product": "Web Compatibility",
        "cf_user_story": "url:angusnicneven.com/*",
        "blocks": [],
        "status": "ASSIGNED",
        "summary": "Javascript causes infinite scroll because event.path is undefined",
        "resolution": "",
        "depends_on": [111111],
        "see_also": [],
        "component": "Desktop",
        "severity": "--",
        "priority": "--",
        "assigned_to": "nobody@mozilla.org",
    },
    {
        "id": 222222,
        "product": "Web Compatibility",
        "cf_user_story": "url:example.com/*",
        "blocks": [],
        "status": "ASSIGNED",
        "summary": "Test breakage bug",
        "resolution": "",
        "depends_on": [111111],
        "see_also": [],
        "component": "Desktop",
        "severity": "--",
        "priority": "--",
        "assigned_to": "nobody@mozilla.org",
    },
]

SAMPLE_CORE_AS_KB_BUGS = [
    {
        "whiteboard": "",
        "see_also": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1740472"],
        "severity": "S3",
        "product": "Core",
        "depends_on": [],
        "summary": "Consider adding support for Error.captureStackTrace",
        "resolution": "",
        "last_change_time": "2024-05-27T15:07:03Z",
        "keywords": ["parity-chrome", "parity-safari", "webcompat:platform-bug"],
        "priority": "P3",
        "creation_time": "2024-03-21T16:40:27Z",
        "cf_user_story": "",
        "status": "NEW",
        "blocks": [1539848, 1729514, 1896383],
        "url": "",
        "cf_last_resolved": None,
        "component": "JavaScript Engine",
        "id": 1886820,
        "assigned_to": "nobody@mozilla.org",
    },
    {
        "depends_on": [1896672],
        "product": "Core",
        "severity": "S2",
        "see_also": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1863217"],
        "whiteboard": "",
        "resolution": "",
        "summary": "Popup blocker is too strict when opening new windows",
        "status": "NEW",
        "cf_user_story": "",
        "priority": "P3",
        "creation_time": "2024-04-30T14:04:23Z",
        "keywords": ["webcompat:platform-bug"],
        "last_change_time": "2024-05-14T15:19:21Z",
        "id": 1894244,
        "component": "DOM: Window and Location",
        "cf_last_resolved": None,
        "url": "",
        "blocks": [1656444, 1835339, 222222],
        "assigned_to": "nobody@mozilla.org",
    },
    {
        "whiteboard": "",
        "see_also": [],
        "severity": "S3",
        "product": "Core",
        "depends_on": [999999],
        "summary": "Example core issue",
        "resolution": "",
        "last_change_time": "2024-05-27T15:07:03Z",
        "keywords": ["webcompat:platform-bug"],
        "priority": "P3",
        "creation_time": "2024-03-21T16:40:27Z",
        "cf_user_story": "",
        "status": "NEW",
        "blocks": [],
        "url": "",
        "cf_last_resolved": None,
        "component": "JavaScript Engine",
        "id": 444444,
        "assigned_to": "nobody@mozilla.org",
    },
]

SAMPLE_HISTORY = [
    {
        "id": 1536482,
        "history": [
            {
                "changes": [
                    {"removed": "--", "field_name": "priority", "added": "P4"},
                    {
                        "added": "1464828, 1529973",
                        "removed": "",
                        "field_name": "depends_on",
                    },
                    {
                        "field_name": "cf_status_firefox68",
                        "removed": "affected",
                        "added": "---",
                    },
                    {
                        "field_name": "keywords",
                        "removed": "",
                        "added": "webcompat:needs-diagnosis",
                    },
                ],
                "when": "2023-05-01T17:41:18Z",
                "who": "example",
            }
        ],
    },
    {
        "id": 1536483,
        "history": [
            {
                "changes": [
                    {
                        "field_name": "cf_user_story",
                        "added": "@@ -0,0 +1,3 @@\n+platform:linux\r\n+impact:feature-broken\r\n+affects:some\n\\ No newline at end of file\n",  # noqa
                        "removed": "",
                    },
                    {"field_name": "priority", "removed": "--", "added": "P3"},
                    {"removed": "--", "added": "S4", "field_name": "severity"},
                ],
                "who": "example",
                "when": "2023-03-18T16:58:27Z",
            },
            {
                "changes": [
                    {
                        "field_name": "status",
                        "added": "ASSIGNED",
                        "removed": "UNCONFIRMED",
                    },
                    {
                        "field_name": "cc",
                        "removed": "",
                        "added": "example@example.com",
                    },
                ],
                "when": "2023-06-01T10:00:00Z",
                "who": "example",
            },
        ],
    },
    {
        "id": 1536484,
        "alias": None,
        "history": [{"changes": [], "when": "2023-07-01T12:00:00Z", "who": "example"}],
    },
    {
        "id": 1536485,
        "alias": None,
        "history": [
            {
                "changes": [
                    {
                        "removed": "",
                        "field_name": "cc",
                        "added": "someone@example.com",
                    },
                    {
                        "removed": "",
                        "field_name": "keywords",
                        "added": "webcompat:platform-bug",
                    },
                ],
                "when": "2023-05-01T14:00:00Z",
                "who": "example",
            },
            {
                "changes": [
                    {
                        "removed": "ASSIGNED",
                        "field_name": "status",
                        "added": "RESOLVED",
                    }
                ],
                "when": "2023-08-01T14:00:00Z",
                "who": "example",
            },
        ],
    },
]

MISSING_KEYWORDS_HISTORY = [
    {
        "id": 1898563,
        "alias": None,
        "history": [
            {
                "when": "2024-05-27T15:10:10Z",
                "changes": [
                    {
                        "added": "@@ -1 +1,4 @@\n-\n+platform:windows,mac,linux,android\r\n+impact:blocked\r\n+configuration:general\r\n+affects:all\n",  # noqa
                        "field_name": "cf_user_story",
                        "removed": "",
                    },
                    {"removed": "--", "field_name": "severity", "added": "S2"},
                    {
                        "removed": "",
                        "added": "name@example.com",
                        "field_name": "cc",
                    },
                    {"added": "P2", "field_name": "priority", "removed": "P1"},
                    {"removed": "", "added": "1886128", "field_name": "depends_on"},
                ],
                "who": "name@example.com",
            }
        ],
    },
    {
        "history": [
            {
                "who": "someone@example.com",
                "when": "2024-05-13T16:03:18Z",
                "changes": [
                    {
                        "field_name": "cf_user_story",
                        "added": "@@ -1 +1,4 @@\n-\n+platform:windows,mac,linux\r\n+impact:site-broken\r\n+configuration:general\r\n+affects:all\n",  # noqa
                        "removed": "",
                    },
                    {"removed": "P3", "added": "P1", "field_name": "priority"},
                    {
                        "removed": "",
                        "field_name": "keywords",
                        "added": "webcompat:needs-diagnosis",
                    },
                    {"added": "S2", "field_name": "severity", "removed": "--"},
                    {
                        "removed": "",
                        "field_name": "cc",
                        "added": "someone@example.com",
                    },
                ],
            },
            {
                "who": "someone@example.com",
                "when": "2024-05-21T17:17:52Z",
                "changes": [
                    {"removed": "", "field_name": "cc", "added": "someone@example.com"}
                ],
            },
            {
                "when": "2024-05-21T17:22:20Z",
                "changes": [
                    {"field_name": "depends_on", "added": "1886820", "removed": ""}
                ],
                "who": "someone@example.com",
            },
            {
                "changes": [
                    {
                        "removed": "webcompat:needs-diagnosis",
                        "field_name": "keywords",
                        "added": "webcompat:needs-sitepatch",
                    },
                    {
                        "added": "someone@example.com",
                        "field_name": "cc",
                        "removed": "",
                    },
                ],
                "when": "2024-05-27T15:07:33Z",
                "who": "someone@example.com",
            },
            {
                "who": "someone@example.com",
                "changes": [
                    {"field_name": "depends_on", "added": "1876368", "removed": ""}
                ],
                "when": "2024-06-05T19:25:37Z",
            },
            {
                "changes": [
                    {"added": "someone@example.com", "field_name": "cc", "removed": ""}
                ],
                "when": "2024-06-09T02:49:27Z",
                "who": "someone@example.com",
            },
            {
                "who": "someone@example.com",
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:sitepatch-applied",
                        "removed": "webcompat:needs-sitepatch",
                    }
                ],
                "when": "2024-06-11T16:34:22Z",
            },
        ],
        "alias": None,
        "id": 1896383,
    },
    {
        "history": [
            {
                "who": "someone@example.com",
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "",
                        "removed": "webcompat:needs-diagnosis",
                    }
                ],
                "when": "2024-06-11T16:34:22Z",
            },
        ],
        "alias": None,
        "id": 222222,
    },
]

MISSING_KEYWORDS_BUGS = [
    {
        "creator": "name@example.com",
        "see_also": ["https://github.com/webcompat/web-bugs/issues/135636"],
        "id": 1898563,
        "component": "Site Reports",
        "keywords": ["webcompat:needs-diagnosis", "webcompat:needs-sitepatch"],
        "resolution": "",
        "summary": "mylotto.co.nz - Website not supported on Firefox",
        "product": "Web Compatibility",
        "creator_detail": {
            "real_name": "Sample",
            "id": 111111,
            "nick": "sample",
            "email": "name@example.com",
            "name": "name@example.com",
        },
        "status": "NEW",
        "depends_on": [1886128],
        "creation_time": "2024-05-23T16:40:29Z",
    },
    {
        "component": "Site Reports",
        "keywords": ["webcompat:sitepatch-applied"],
        "see_also": ["https://github.com/webcompat/web-bugs/issues/136865"],
        "id": 1896383,
        "creator": "name@example.com",
        "depends_on": [1886820, 1876368],
        "status": "NEW",
        "product": "Web Compatibility",
        "creator_detail": {
            "name": "name@example.com",
            "id": 111111,
            "email": "name@example.com",
            "nick": "sample",
            "real_name": "Sample",
        },
        "resolution": "",
        "summary": "www.unimarc.cl - Buttons not working",
        "creation_time": "2024-05-13T13:02:11Z",
    },
    {
        "id": 222222,
        "product": "Web Compatibility",
        "blocks": [],
        "status": "ASSIGNED",
        "summary": "Test breakage bug",
        "resolution": "",
        "depends_on": [111111],
        "see_also": [],
        "component": "Desktop",
        "severity": "--",
        "priority": "--",
        "creator_detail": {
            "name": "name@example.com",
            "id": 111111,
            "email": "name@example.com",
            "nick": "sample",
            "real_name": "Sample",
        },
        "creator": "name@example.com",
        "creation_time": "2024-05-13T13:02:11Z",
        "keywords": [],
    },
]

REMOVED_READDED_BUGS = [
    {
        "id": 333333,
        "product": "Web Compatibility",
        "blocks": [],
        "status": "ASSIGNED",
        "summary": "Test breakage bug",
        "resolution": "",
        "depends_on": [111111],
        "see_also": [],
        "component": "Desktop",
        "severity": "--",
        "priority": "--",
        "creator_detail": {
            "name": "name@example.com",
            "id": 111111,
            "email": "name@example.com",
            "nick": "sample",
            "real_name": "Sample",
        },
        "creator": "name@example.com",
        "creation_time": "2024-05-13T13:02:11Z",
        "keywords": ["webcompat:needs-diagnosis"],
    }
]

REMOVED_READDED_HISTORY = [
    {
        "history": [
            {
                "who": "someone@example.com",
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "",
                        "removed": "webcompat:needs-diagnosis",
                    }
                ],
                "when": "2024-06-11T16:34:22Z",
            },
            {
                "who": "someone@example.com",
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:needs-sitepatch",
                        "removed": "",
                    }
                ],
                "when": "2024-06-15T16:34:22Z",
            },
            {
                "who": "someone@example.com",
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:needs-diagnosis",
                        "removed": "",
                    }
                ],
                "when": "2024-07-11T16:34:22Z",
            },
            {
                "who": "someone@example.com",
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "",
                        "removed": "webcompat:needs-sitepatch",
                    }
                ],
                "when": "2024-07-14T16:34:22Z",
            },
            {
                "who": "someone@example.com",
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "",
                        "removed": "webcompat:needs-diagnosis",
                    }
                ],
                "when": "2024-09-11T16:34:22Z",
            },
            {
                "who": "someone@example.com",
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:needs-diagnosis",
                        "removed": "",
                    }
                ],
                "when": "2024-12-11T16:34:22Z",
            },
        ],
        "alias": None,
        "id": 333333,
    },
]

KEYWORDS_AND_STATUS = [
    {
        "history": [
            {
                "changes": [
                    {
                        "added": "parity-chrome, parity-edge, parity-ie",
                        "field_name": "keywords",
                        "removed": "",
                    },
                ],
                "who": "someone@example.com",
                "when": "2018-05-02T18:25:47Z",
            },
            {
                "changes": [
                    {"added": "RESOLVED", "removed": "NEW", "field_name": "status"}
                ],
                "when": "2024-05-16T10:58:15Z",
                "who": "someone@example.com",
            },
            {
                "who": "someone@example.com",
                "when": "2024-06-03T14:44:48Z",
                "changes": [
                    {
                        "removed": "RESOLVED",
                        "field_name": "status",
                        "added": "REOPENED",
                    },
                    {
                        "field_name": "keywords",
                        "removed": "",
                        "added": "webcompat:platform-bug",
                    },
                ],
            },
            {
                "when": "2016-01-14T14:01:36Z",
                "who": "someone@example.com",
                "changes": [
                    {
                        "added": "NEW",
                        "removed": "UNCONFIRMED",
                        "field_name": "status",
                    }
                ],
            },
        ],
        "alias": None,
        "id": 1239595,
    },
]


@pytest.fixture(scope="module")
def bz():
    with patch("webcompat_kb.main.bigquery.Client") as mock_bq:
        mock_bq.return_value = Mock()
        return BugzillaToBigQuery(
            bq_project_id="placeholder_id",
            bq_dataset_id="placeholder_dataset",
            bugzilla_api_key="placeholder_key",
        )


def test_extract_int_from_field():
    field = extract_int_from_field("P3")
    assert field == 3

    field = extract_int_from_field("critical")
    assert field == 1

    field = extract_int_from_field("--")
    assert field is None

    field = extract_int_from_field("N/A")
    assert field is None

    field = extract_int_from_field("")
    assert field is None

    field = extract_int_from_field(None)
    assert field is None


def test_process_fields_with_no_bugs(bz):
    result = bz.process_fields([], RELATION_CONFIG)
    expected = ({}, {})
    assert result == expected


def test_process_fields(bz):
    bugs, ids = bz.process_fields(SAMPLE_BUGS, RELATION_CONFIG)
    expected_processed_bugs = {
        1835339: {
            "core_bugs": [903746],
            "breakage_reports": [],
            "interventions": [
                "https://github.com/mozilla-extensions/webcompat-addon/blob/5b391018e847a1eb30eba4784c86acd1c638ed26/src/injections/js/bug1739489-draftjs-beforeinput.js"  # noqa
            ],
            "other_browser_issues": [],
            "standards_issues": [],
            "standards_positions": [],
        },
        1835416: {
            "core_bugs": [],
            "breakage_reports": [],
            "interventions": [],
            "other_browser_issues": [],
            "standards_issues": [],
            "standards_positions": [
                "https://mozilla.github.io/standards-positions/#webusb"
            ],
        },
        111111: {
            "core_bugs": [555555],
            "breakage_reports": [222222, 1734557],
            "interventions": [],
            "other_browser_issues": ["https://crbug.com/606208"],
            "standards_issues": ["https://github.com/whatwg/html/issues/1896"],
            "standards_positions": [
                "https://github.com/mozilla/standards-positions/issues/20",
                "https://github.com/WebKit/standards-positions/issues/186",
            ],
        },
    }

    expected_bug_ids = {
        "core": [903746, 555555],
        "breakage": [222222, 1734557],
    }

    assert bugs == expected_processed_bugs
    assert ids == expected_bug_ids


def test_relations(bz):
    bugs, _ = bz.process_fields(SAMPLE_BUGS, RELATION_CONFIG)
    relations = bz.build_relations(bugs, RELATION_CONFIG)

    assert relations["core_bugs"] == [
        {"knowledge_base_bug": 1835339, "core_bug": 903746},
        {"knowledge_base_bug": 111111, "core_bug": 555555},
    ]

    assert relations["breakage_reports"] == [
        {"knowledge_base_bug": 111111, "breakage_bug": 222222},
        {"knowledge_base_bug": 111111, "breakage_bug": 1734557},
    ]

    assert relations["interventions"] == [
        {
            "knowledge_base_bug": 1835339,
            "code_url": "https://github.com/mozilla-extensions/webcompat-addon/blob/5b391018e847a1eb30eba4784c86acd1c638ed26/src/injections/js/bug1739489-draftjs-beforeinput.js",  # noqa
        }
    ]
    assert relations["other_browser_issues"] == [
        {"knowledge_base_bug": 111111, "issue_url": "https://crbug.com/606208"}
    ]
    assert relations["standards_issues"] == [
        {
            "knowledge_base_bug": 111111,
            "issue_url": "https://github.com/whatwg/html/issues/1896",
        }
    ]
    assert relations["standards_positions"] == [
        {
            "knowledge_base_bug": 1835416,
            "discussion_url": "https://mozilla.github.io/standards-positions/#webusb",  # noqa
        },
        {
            "knowledge_base_bug": 111111,
            "discussion_url": "https://github.com/mozilla/standards-positions/issues/20",  # noqa
        },
        {
            "knowledge_base_bug": 111111,
            "discussion_url": "https://github.com/WebKit/standards-positions/issues/186",  # noqa
        },
    ]


def test_add_links(bz):
    bugs, _ = bz.process_fields(SAMPLE_BUGS, RELATION_CONFIG)
    core_bugs, _ = bz.process_fields(
        SAMPLE_CORE_BUGS, {key: RELATION_CONFIG[key] for key in LINK_FIELDS}
    )

    result = bz.add_links(bugs, core_bugs)

    assert result[1835339]["standards_issues"] == [
        "https://github.com/w3c/uievents/issues/353"
    ]
    assert result[111111]["standards_positions"] == [
        "https://github.com/mozilla/standards-positions/issues/20",
        "https://github.com/WebKit/standards-positions/issues/186",
        "https://mozilla.github.io/standards-positions/#testposition",
    ]


def test_add_links_no_core(bz):
    bugs, _ = bz.process_fields(SAMPLE_BUGS, RELATION_CONFIG)
    core_bugs, _ = bz.process_fields(SAMPLE_CORE_BUGS, RELATION_CONFIG)

    result = bz.add_links(bugs, {})

    assert result[1835339]["standards_issues"] == []
    assert result[111111]["standards_positions"] == [
        "https://github.com/mozilla/standards-positions/issues/20",
        "https://github.com/WebKit/standards-positions/issues/186",
    ]


def test_get_bugs_updated_since_last_import(bz):
    all_bugs = [
        {"id": 1, "last_change_time": "2023-04-01T10:00:00Z"},
        {"id": 2, "last_change_time": "2023-04-02T11:30:00Z"},
        {"id": 3, "last_change_time": "2023-04-03T09:45:00Z"},
    ]

    last_import_time = datetime(2023, 4, 2, 10, 0, tzinfo=timezone.utc)
    expected_result = [2, 3]
    result = bz.get_bugs_updated_since_last_import(all_bugs, last_import_time)
    assert result == expected_result


def test_filter_bug_history_changes(bz):
    expected_result = [
        {
            "number": 1536482,
            "who": "example",
            "change_time": "2023-05-01T17:41:18Z",
            "changes": [
                {
                    "field_name": "keywords",
                    "removed": "",
                    "added": "webcompat:needs-diagnosis",
                }
            ],
        },
        {
            "number": 1536483,
            "who": "example",
            "change_time": "2023-06-01T10:00:00Z",
            "changes": [
                {
                    "field_name": "status",
                    "added": "ASSIGNED",
                    "removed": "UNCONFIRMED",
                }
            ],
        },
        {
            "number": 1536485,
            "who": "example",
            "change_time": "2023-05-01T14:00:00Z",
            "changes": [
                {
                    "removed": "",
                    "field_name": "keywords",
                    "added": "webcompat:platform-bug",
                }
            ],
        },
        {
            "number": 1536485,
            "who": "example",
            "change_time": "2023-08-01T14:00:00Z",
            "changes": [
                {"removed": "ASSIGNED", "field_name": "status", "added": "RESOLVED"}
            ],
        },
    ]

    result, bug_ids = bz.extract_relevant_fields(SAMPLE_HISTORY)
    assert result == expected_result
    assert bug_ids == {1536482, 1536483, 1536485}


def test_create_synthetic_history(bz):
    history, bug_ids = bz.extract_relevant_fields(MISSING_KEYWORDS_HISTORY)
    result = bz.create_synthetic_history(MISSING_KEYWORDS_BUGS, history)

    expected = [
        {
            "number": 1898563,
            "who": "name@example.com",
            "change_time": "2024-05-23T16:40:29Z",
            "changes": [
                {
                    "added": "webcompat:needs-diagnosis, webcompat:needs-sitepatch",
                    "field_name": "keywords",
                    "removed": "",
                }
            ],
        },
        {
            "number": 222222,
            "who": "name@example.com",
            "change_time": "2024-05-13T13:02:11Z",
            "changes": [
                {
                    "added": "webcompat:needs-diagnosis",
                    "field_name": "keywords",
                    "removed": "",
                }
            ],
        },
    ]

    assert result == expected


def test_create_synthetic_history_removed_readded(bz):
    history, bug_ids = bz.extract_relevant_fields(REMOVED_READDED_HISTORY)
    result = bz.create_synthetic_history(REMOVED_READDED_BUGS, history)

    expected = [
        {
            "number": 333333,
            "who": "name@example.com",
            "change_time": "2024-05-13T13:02:11Z",
            "changes": [
                {
                    "added": "webcompat:needs-diagnosis",
                    "field_name": "keywords",
                    "removed": "",
                }
            ],
        }
    ]

    assert result == expected


def test_is_removed_earliest(bz):
    keyword_map = {
        "added": {
            "webcompat:needs-sitepatch": [
                datetime(2024, 6, 15, 16, 34, 22, tzinfo=timezone.utc)
            ],
            "webcompat:needs-diagnosis": [
                datetime(2024, 7, 11, 16, 34, 22, tzinfo=timezone.utc),
                datetime(2024, 12, 11, 16, 34, 22, tzinfo=timezone.utc),
            ],
        },
        "removed": {
            "webcompat:needs-diagnosis": [
                datetime(2024, 6, 11, 16, 34, 22, tzinfo=timezone.utc),
                datetime(2024, 9, 11, 16, 34, 22, tzinfo=timezone.utc),
            ],
            "webcompat:needs-sitepatch": [
                datetime(2024, 7, 14, 16, 34, 22, tzinfo=timezone.utc)
            ],
        },
    }

    is_removed_first_diagnosis = bz.is_removed_earliest(
        keyword_map["added"]["webcompat:needs-diagnosis"],
        keyword_map["removed"]["webcompat:needs-diagnosis"],
    )

    is_removed_first_sitepatch = bz.is_removed_earliest(
        keyword_map["added"]["webcompat:needs-sitepatch"],
        keyword_map["removed"]["webcompat:needs-sitepatch"],
    )

    is_removed_first_empty_added = bz.is_removed_earliest(
        [],
        [datetime(2024, 7, 14, 16, 34, 22, tzinfo=timezone.utc)],
    )

    is_removed_first_empty_removed = bz.is_removed_earliest(
        [datetime(2024, 7, 14, 16, 34, 22, tzinfo=timezone.utc)],
        [],
    )

    is_removed_first_empty = bz.is_removed_earliest(
        [],
        [],
    )

    assert is_removed_first_diagnosis
    assert not is_removed_first_sitepatch
    assert is_removed_first_empty_added
    assert not is_removed_first_empty_removed
    assert not is_removed_first_empty


@patch("webcompat_kb.main.BugzillaToBigQuery.get_existing_history_records_by_ids")
def test_filter_only_unsaved_changes(mock_get_existing, bz):
    schema = {"number": 0, "who": 1, "change_time": 2, "changes": 3}

    mock_get_existing.return_value = [
        Row(
            (
                1896383,
                "someone@example.com",
                datetime(2024, 5, 27, 15, 7, 33, tzinfo=timezone.utc),
                [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:needs-sitepatch",
                        "removed": "webcompat:needs-diagnosis",
                    }
                ],
            ),
            schema,
        ),
        Row(
            (
                1896383,
                "someone@example.com",
                datetime(2024, 6, 11, 16, 34, 22, tzinfo=timezone.utc),
                [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:sitepatch-applied",
                        "removed": "webcompat:needs-sitepatch",
                    }
                ],
            ),
            schema,
        ),
    ]

    history, bug_ids = bz.extract_relevant_fields(MISSING_KEYWORDS_HISTORY)
    result = bz.filter_only_unsaved_changes(history, bug_ids)

    expected = [
        {
            "number": 1896383,
            "who": "someone@example.com",
            "change_time": "2024-05-13T16:03:18Z",
            "changes": [
                {
                    "removed": "",
                    "field_name": "keywords",
                    "added": "webcompat:needs-diagnosis",
                }
            ],
        },
        {
            "number": 222222,
            "who": "someone@example.com",
            "change_time": "2024-06-11T16:34:22Z",
            "changes": [
                {
                    "field_name": "keywords",
                    "added": "",
                    "removed": "webcompat:needs-diagnosis",
                }
            ],
        },
    ]

    result.sort(key=lambda item: item["number"])
    expected.sort(key=lambda item: item["number"])

    assert result == expected


@patch("webcompat_kb.main.BugzillaToBigQuery.get_existing_history_records_by_ids")
def test_filter_only_unsaved_changes_multiple_changes(mock_get_existing, bz):
    schema = {"number": 0, "who": 1, "change_time": 2, "changes": 3}

    mock_get_existing.return_value = [
        Row(
            (
                1239595,
                "someone@example.com",
                datetime(2018, 5, 2, 18, 25, 47, tzinfo=timezone.utc),
                [
                    {
                        "field_name": "keywords",
                        "added": "parity-chrome, parity-edge, parity-ie",
                        "removed": "",
                    }
                ],
            ),
            schema,
        ),
        Row(
            (
                1239595,
                "someone@example.com",
                datetime(2016, 1, 14, 14, 1, 36, tzinfo=timezone.utc),
                [{"field_name": "status", "added": "NEW", "removed": "UNCONFIRMED"}],
            ),
            schema,
        ),
        Row(
            (
                1239595,
                "someone@example.com",
                datetime(2024, 5, 16, 10, 58, 15, tzinfo=timezone.utc),
                [{"field_name": "status", "added": "RESOLVED", "removed": "NEW"}],
            ),
            schema,
        ),
    ]

    history, bug_ids = bz.extract_relevant_fields(KEYWORDS_AND_STATUS)
    result = bz.filter_only_unsaved_changes(history, bug_ids)
    changes = result[0]["changes"]

    expected_changes = [
        {
            "field_name": "keywords",
            "added": "webcompat:platform-bug",
            "removed": "",
        },
        {"field_name": "status", "added": "REOPENED", "removed": "RESOLVED"},
    ]

    changes.sort(key=lambda item: item["field_name"])
    expected_changes.sort(key=lambda item: item["field_name"])

    assert len(result) == 1
    assert changes == expected_changes


@patch("webcompat_kb.main.BugzillaToBigQuery.get_existing_history_records_by_ids")
def test_filter_only_unsaved_changes_empty(mock_get_existing, bz):
    mock_get_existing.return_value = []

    history, bug_ids = bz.extract_relevant_fields(MISSING_KEYWORDS_HISTORY)
    result = bz.filter_only_unsaved_changes(history, bug_ids)

    expected = [
        {
            "number": 1896383,
            "who": "someone@example.com",
            "change_time": "2024-05-13T16:03:18Z",
            "changes": [
                {
                    "removed": "",
                    "field_name": "keywords",
                    "added": "webcompat:needs-diagnosis",
                }
            ],
        },
        {
            "number": 1896383,
            "who": "someone@example.com",
            "change_time": "2024-05-27T15:07:33Z",
            "changes": [
                {
                    "removed": "webcompat:needs-diagnosis",
                    "field_name": "keywords",
                    "added": "webcompat:needs-sitepatch",
                }
            ],
        },
        {
            "number": 1896383,
            "who": "someone@example.com",
            "change_time": "2024-06-11T16:34:22Z",
            "changes": [
                {
                    "field_name": "keywords",
                    "added": "webcompat:sitepatch-applied",
                    "removed": "webcompat:needs-sitepatch",
                }
            ],
        },
        {
            "number": 222222,
            "who": "someone@example.com",
            "change_time": "2024-06-11T16:34:22Z",
            "changes": [
                {
                    "field_name": "keywords",
                    "added": "",
                    "removed": "webcompat:needs-diagnosis",
                }
            ],
        },
    ]

    assert result == expected


@patch("webcompat_kb.main.BugzillaToBigQuery.get_existing_history_records_by_ids")
def test_filter_only_unsaved_changes_synthetic(mock_get_existing, bz):
    history, bug_ids = bz.extract_relevant_fields(MISSING_KEYWORDS_HISTORY)
    s_history = bz.create_synthetic_history(MISSING_KEYWORDS_BUGS, history)

    schema = {"number": 0, "who": 1, "change_time": 2, "changes": 3}

    mock_get_existing.return_value = [
        Row(
            (
                1898563,
                "name@example.com",
                datetime(2024, 5, 23, 16, 40, 29, tzinfo=timezone.utc),
                [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:needs-diagnosis, webcompat:needs-sitepatch",  # noqa
                        "removed": "",
                    }
                ],
            ),
            schema,
        )
    ]

    result = bz.filter_only_unsaved_changes(s_history, bug_ids)

    expected = [
        {
            "number": 222222,
            "who": "name@example.com",
            "change_time": "2024-05-13T13:02:11Z",
            "changes": [
                {
                    "added": "webcompat:needs-diagnosis",
                    "field_name": "keywords",
                    "removed": "",
                }
            ],
        }
    ]

    assert result == expected


def test_empty_input():
    assert parse_string_to_json("") == ""


def test_null_input():
    assert parse_string_to_json(None) == ""


def test_single_key_value_pair():
    input_str = "key:value"
    expected = {"key": "value"}
    assert parse_string_to_json(input_str) == expected


def test_multiple_key_value_pairs():
    input_str = "key1:value1\nkey2:value2"
    expected = {"key1": "value1", "key2": "value2"}
    assert parse_string_to_json(input_str) == expected


def test_multiple_values_for_same_key():
    input_str = "key:value1\r\nkey:value2"
    expected = {"key": ["value1", "value2"]}
    assert parse_string_to_json(input_str) == expected


def test_mixed_line_breaks():
    input_str = "key1:value1\r\nkey2:value2\nkey3:value3"
    expected = {"key1": "value1", "key2": "value2", "key3": "value3"}
    assert parse_string_to_json(input_str) == expected


def test_empty_result():
    input_str = "\n\n"
    assert parse_string_to_json(input_str) == ""


def test_severity_string():
    input_str = "platform:linux\r\nimpact:feature-broken\r\naffects:some"
    expected = {
        "platform": "linux",
        "impact": "feature-broken",
        "affects": "some",
    }
    assert parse_string_to_json(input_str) == expected


def test_values_with_colon():
    input_str = "url:http://chatgpt-tokenizer.com/*\r\nurl:excalidraw.com/*\r\nurl:godbolt.org/*\r\nurl:youwouldntsteala.website/*\r\nurl:yandex.ru/images/*"  # noqa
    expected = {
        "url": [
            "http://chatgpt-tokenizer.com/*",
            "excalidraw.com/*",
            "godbolt.org/*",
            "youwouldntsteala.website/*",
            "yandex.ru/images/*",
        ]
    }
    assert parse_string_to_json(input_str) == expected


def test_filter_core_as_kb_bugs(bz):
    core_as_kb_bugs = bz.filter_core_as_kb_bugs(
        SAMPLE_CORE_AS_KB_BUGS, {1835339}, {1896383, 222222}
    )

    assert core_as_kb_bugs[0] == [
        {
            "assigned_to": "nobody@mozilla.org",
            "whiteboard": "",
            "see_also": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1740472"],
            "severity": "S3",
            "product": "Core",
            "depends_on": [],
            "summary": "Consider adding support for Error.captureStackTrace",
            "resolution": "",
            "last_change_time": "2024-05-27T15:07:03Z",
            "keywords": [
                "parity-chrome",
                "parity-safari",
                "webcompat:platform-bug",
            ],
            "priority": "P3",
            "creation_time": "2024-03-21T16:40:27Z",
            "cf_user_story": "",
            "status": "NEW",
            "blocks": [1896383],
            "url": "",
            "cf_last_resolved": None,
            "component": "JavaScript Engine",
            "id": 1886820,
        },
        {
            "assigned_to": "nobody@mozilla.org",
            "whiteboard": "",
            "see_also": [],
            "severity": "S3",
            "product": "Core",
            "depends_on": [999999],
            "summary": "Example core issue",
            "resolution": "",
            "last_change_time": "2024-05-27T15:07:03Z",
            "keywords": ["webcompat:platform-bug"],
            "priority": "P3",
            "creation_time": "2024-03-21T16:40:27Z",
            "cf_user_story": "",
            "status": "NEW",
            "blocks": [],
            "url": "",
            "cf_last_resolved": None,
            "component": "JavaScript Engine",
            "id": 444444,
        },
    ]

    assert core_as_kb_bugs[1] == {999999}


def test_convert_bug_data(bz):
    expected_data = [
        {
            "assigned_to": "test@example.org",
            "component": "Knowledge Base",
            "creation_time": "2000-07-25T13:50:04Z",
            "keywords": [],
            "number": 1835339,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "resolved_time": None,
            "severity": None,
            "status": "NEW",
            "title": "Missing implementation of textinput event",
            "url": "",
            "user_story": {
                "url": [
                    "cmcreg.bancosantander.es/*",
                    "new.reddit.com/*",
                    "web.whatsapp.com/*",
                    "facebook.com/*",
                    "twitter.com/*",
                    "reddit.com/*",
                    "mobilevikings.be/*",
                    "book.ersthelfer.tv/*",
                ],
            },
            "whiteboard": "",
        },
        {
            "assigned_to": None,
            "component": "Knowledge Base",
            "creation_time": "2000-07-25T13:50:04Z",
            "keywords": [],
            "number": 1835416,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "resolved_time": None,
            "severity": None,
            "status": "NEW",
            "title": "Sites breaking due to the lack of WebUSB support",
            "url": "",
            "user_story": {
                "url": [
                    "webminidisc.com/*",
                    "app.webadb.com/*",
                    "www.numworks.com/*",
                    "webadb.github.io/*",
                    "www.stemplayer.com/*",
                    "wootility.io/*",
                    "python.microbit.org/*",
                    "flash.android.com/*",
                ],
            },
            "whiteboard": "",
        },
        {
            "assigned_to": None,
            "component": "Knowledge Base",
            "creation_time": "2000-07-25T13:50:04Z",
            "keywords": [],
            "number": 111111,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "resolved_time": None,
            "severity": None,
            "status": "NEW",
            "title": "Test bug",
            "url": "",
            "user_story": "",
            "whiteboard": "",
        },
    ]
    for bug, expected in zip(SAMPLE_BUGS, expected_data):
        assert bz.convert_bug_data(bug) == expected


def test_parse_datetime():
    result = parse_datetime_str("2024-06-11T16:35:50Z")
    assert result == datetime(2024, 6, 11, 16, 35, 50, tzinfo=timezone.utc)
