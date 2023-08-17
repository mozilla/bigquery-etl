from webcompat_kb.main import process_fields
from webcompat_kb.main import extract_int_from_field
from webcompat_kb.main import build_relations, merge_relations
from webcompat_kb.main import add_links
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


class TestMain:
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
        result = process_fields([], RELATION_CONFIG)
        expected = ({}, {})
        assert result == expected

    def test_process_fields(self):
        bugs, ids = process_fields(SAMPLE_BUGS, RELATION_CONFIG)
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
                "url_patterns": [
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
            1835416: {
                "core_bugs": [],
                "breakage_reports": [],
                "interventions": [],
                "other_browser_issues": [],
                "standards_issues": [],
                "standards_positions": [
                    "https://mozilla.github.io/standards-positions/#webusb"
                ],
                "url_patterns": [
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
                "url_patterns": [],
            },
        }

        expected_bug_ids = {
            "core": [903746, 555555],
            "breakage": [222222, 1734557],
        }

        assert bugs == expected_processed_bugs
        assert ids == expected_bug_ids

    def test_process_fields_config_variation(self):
        bugs, ids = process_fields(
            SAMPLE_BREAKAGE_BUGS, {"url_patterns": RELATION_CONFIG["url_patterns"]}
        )
        expected = {
            1734557: {"url_patterns": ["angusnicneven.com/*"]},
            222222: {"url_patterns": ["example.com/*"]},
        }

        assert bugs == expected
        assert ids == {}

    def test_relations(self):
        bugs, _ = process_fields(SAMPLE_BUGS, RELATION_CONFIG)
        relations = build_relations(bugs, RELATION_CONFIG)

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
        assert relations["url_patterns"] == [
            {
                "bug": 1835339,
                "url_pattern": "cmcreg.bancosantander.es/*",
            },
            {"bug": 1835339, "url_pattern": "new.reddit.com/*"},
            {"bug": 1835339, "url_pattern": "web.whatsapp.com/*"},
            {"bug": 1835339, "url_pattern": "facebook.com/*"},
            {"bug": 1835339, "url_pattern": "twitter.com/*"},
            {"bug": 1835339, "url_pattern": "reddit.com/*"},
            {"bug": 1835339, "url_pattern": "mobilevikings.be/*"},
            {"bug": 1835339, "url_pattern": "book.ersthelfer.tv/*"},
            {"bug": 1835416, "url_pattern": "webminidisc.com/*"},
            {"bug": 1835416, "url_pattern": "app.webadb.com/*"},
            {"bug": 1835416, "url_pattern": "www.numworks.com/*"},
            {"bug": 1835416, "url_pattern": "webadb.github.io/*"},
            {"bug": 1835416, "url_pattern": "www.stemplayer.com/*"},
            {"bug": 1835416, "url_pattern": "wootility.io/*"},
            {"bug": 1835416, "url_pattern": "python.microbit.org/*"},
            {"bug": 1835416, "url_pattern": "flash.android.com/*"},
        ]

    def test_relations_config_variation(self):
        url_only_config = {"url_patterns": RELATION_CONFIG["url_patterns"]}
        bugs, _ = process_fields(SAMPLE_BREAKAGE_BUGS, url_only_config)
        relations = build_relations(bugs, url_only_config)

        assert len(relations) == 1
        assert "url_patterns" in relations

        assert relations["url_patterns"] == [
            {"bug": 1734557, "url_pattern": "angusnicneven.com/*"},
            {"bug": 222222, "url_pattern": "example.com/*"},
        ]

    def test_add_links(self):
        bugs, _ = process_fields(SAMPLE_BUGS, RELATION_CONFIG)
        core_bugs, _ = process_fields(
            SAMPLE_CORE_BUGS, {key: RELATION_CONFIG[key] for key in LINK_FIELDS}
        )

        result = add_links(bugs, core_bugs)

        assert result[1835339]["standards_issues"] == [
            "https://github.com/w3c/uievents/issues/353"
        ]
        assert result[111111]["standards_positions"] == [
            "https://github.com/mozilla/standards-positions/issues/20",
            "https://github.com/WebKit/standards-positions/issues/186",
            "https://mozilla.github.io/standards-positions/#testposition",
        ]

    def test_add_links_no_core(self):
        bugs, _ = process_fields(SAMPLE_BUGS, RELATION_CONFIG)
        core_bugs, _ = process_fields(SAMPLE_CORE_BUGS, RELATION_CONFIG)

        result = add_links(bugs, {})

        assert result[1835339]["standards_issues"] == []
        assert result[111111]["standards_positions"] == [
            "https://github.com/mozilla/standards-positions/issues/20",
            "https://github.com/WebKit/standards-positions/issues/186",
        ]

    def test_merge_relations(self):
        bugs, _ = process_fields(SAMPLE_BUGS, RELATION_CONFIG)
        relations = build_relations(bugs, RELATION_CONFIG)

        url_only_config = {"url_patterns": RELATION_CONFIG["url_patterns"]}
        breakage_bugs, _ = process_fields(SAMPLE_BREAKAGE_BUGS, url_only_config)
        breakage_relations = build_relations(breakage_bugs, url_only_config)

        merged = merge_relations(relations, breakage_relations)

        assert merged["url_patterns"] == [
            {"bug": 1835339, "url_pattern": "cmcreg.bancosantander.es/*"},
            {"bug": 1835339, "url_pattern": "new.reddit.com/*"},
            {"bug": 1835339, "url_pattern": "web.whatsapp.com/*"},
            {"bug": 1835339, "url_pattern": "facebook.com/*"},
            {"bug": 1835339, "url_pattern": "twitter.com/*"},
            {"bug": 1835339, "url_pattern": "reddit.com/*"},
            {"bug": 1835339, "url_pattern": "mobilevikings.be/*"},
            {"bug": 1835339, "url_pattern": "book.ersthelfer.tv/*"},
            {"bug": 1835416, "url_pattern": "webminidisc.com/*"},
            {"bug": 1835416, "url_pattern": "app.webadb.com/*"},
            {"bug": 1835416, "url_pattern": "www.numworks.com/*"},
            {"bug": 1835416, "url_pattern": "webadb.github.io/*"},
            {"bug": 1835416, "url_pattern": "www.stemplayer.com/*"},
            {"bug": 1835416, "url_pattern": "wootility.io/*"},
            {"bug": 1835416, "url_pattern": "python.microbit.org/*"},
            {"bug": 1835416, "url_pattern": "flash.android.com/*"},
            {"bug": 1734557, "url_pattern": "angusnicneven.com/*"},
            {"bug": 222222, "url_pattern": "example.com/*"},
        ]
