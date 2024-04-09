from datetime import datetime, timezone
from unittest.mock import Mock, patch
from unittest import TestCase

from webcompat_kb.main import BugzillaToBigQuery
from webcompat_kb.main import extract_int_from_field
from webcompat_kb.main import parse_string_to_json
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


class TestMain(TestCase):
    @patch("webcompat_kb.main.bigquery.Client")
    def setUp(self, mock_bq):
        mock_bq.return_value = Mock()
        self.bz = BugzillaToBigQuery(
            bq_project_id="placeholder_id",
            bq_dataset_id="placeholder_dataset",
            bugzilla_api_key="placeholder_key",
        )

    def test_extract_int_from_field(self):
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

    def test_process_fields_with_no_bugs(self):
        result = self.bz.process_fields([], RELATION_CONFIG)
        expected = ({}, {})
        assert result == expected

    def test_process_fields(self):
        bugs, ids = self.bz.process_fields(SAMPLE_BUGS, RELATION_CONFIG)
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

    def test_relations(self):
        bugs, _ = self.bz.process_fields(SAMPLE_BUGS, RELATION_CONFIG)
        relations = self.bz.build_relations(bugs, RELATION_CONFIG)

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

    def test_add_links(self):
        bugs, _ = self.bz.process_fields(SAMPLE_BUGS, RELATION_CONFIG)
        core_bugs, _ = self.bz.process_fields(
            SAMPLE_CORE_BUGS, {key: RELATION_CONFIG[key] for key in LINK_FIELDS}
        )

        result = self.bz.add_links(bugs, core_bugs)

        assert result[1835339]["standards_issues"] == [
            "https://github.com/w3c/uievents/issues/353"
        ]
        assert result[111111]["standards_positions"] == [
            "https://github.com/mozilla/standards-positions/issues/20",
            "https://github.com/WebKit/standards-positions/issues/186",
            "https://mozilla.github.io/standards-positions/#testposition",
        ]

    def test_add_links_no_core(self):
        bugs, _ = self.bz.process_fields(SAMPLE_BUGS, RELATION_CONFIG)
        core_bugs, _ = self.bz.process_fields(SAMPLE_CORE_BUGS, RELATION_CONFIG)

        result = self.bz.add_links(bugs, {})

        assert result[1835339]["standards_issues"] == []
        assert result[111111]["standards_positions"] == [
            "https://github.com/mozilla/standards-positions/issues/20",
            "https://github.com/WebKit/standards-positions/issues/186",
        ]

    def test_get_bugs_updated_since_last_import(self):
        all_bugs = [
            {"id": 1, "last_change_time": "2023-04-01T10:00:00Z"},
            {"id": 2, "last_change_time": "2023-04-02T11:30:00Z"},
            {"id": 3, "last_change_time": "2023-04-03T09:45:00Z"},
        ]

        last_import_time = datetime(2023, 4, 2, 10, 0, tzinfo=timezone.utc)
        expected_result = [2, 3]
        result = self.bz.get_bugs_updated_since_last_import(all_bugs, last_import_time)
        self.assertEqual(result, expected_result)

    def test_filter_bug_history_changes(self):
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

        result = self.bz.filter_bug_history_changes(SAMPLE_HISTORY)
        self.assertEqual(result, expected_result)

    def test_empty_input(self):
        self.assertEqual(parse_string_to_json(""), "")

    def test_null_input(self):
        self.assertEqual(parse_string_to_json(None), "")

    def test_single_key_value_pair(self):
        input_str = "key:value"
        expected = {"key": "value"}
        self.assertEqual(parse_string_to_json(input_str), expected)

    def test_multiple_key_value_pairs(self):
        input_str = "key1:value1\nkey2:value2"
        expected = {"key1": "value1", "key2": "value2"}
        self.assertEqual(parse_string_to_json(input_str), expected)

    def test_multiple_values_for_same_key(self):
        input_str = "key:value1\r\nkey:value2"
        expected = {"key": ["value1", "value2"]}
        self.assertEqual(parse_string_to_json(input_str), expected)

    def test_mixed_line_breaks(self):
        input_str = "key1:value1\r\nkey2:value2\nkey3:value3"
        expected = {"key1": "value1", "key2": "value2", "key3": "value3"}
        self.assertEqual(parse_string_to_json(input_str), expected)

    def test_empty_result(self):
        input_str = "\n\n"
        self.assertEqual(parse_string_to_json(input_str), "")

    def test_severity_string(self):
        input_str = "platform:linux\r\nimpact:feature-broken\r\naffects:some"
        expected = {
            "platform": "linux",
            "impact": "feature-broken",
            "affects": "some",
        }
        self.assertEqual(parse_string_to_json(input_str), expected)

    def test_values_with_colon(self):
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
        self.assertEqual(parse_string_to_json(input_str), expected)
