import pytest

from bigquery_etl.jira.adf import adf_to_text


def doc(*content):
    return {"type": "doc", "version": 1, "content": list(content)}


def para(*content):
    return {"type": "paragraph", "content": list(content)}


def text(value, **attrs):
    node = {"type": "text", "text": value}
    node.update(attrs)
    return node


def test_none_and_empty_are_empty_string():
    assert adf_to_text(None) == ""
    assert adf_to_text({}) == ""
    assert adf_to_text(doc()) == ""


def test_a_string_body_is_passed_through():
    """API v2 and some webhooks return a plain string rather than an ADF document."""
    assert adf_to_text("already plain text") == "already plain text"


def test_single_paragraph():
    assert adf_to_text(doc(para(text("Triage notes: making this a p1.")))) == (
        "Triage notes: making this a p1."
    )


def test_adjacent_text_nodes_concatenate_without_separator():
    assert adf_to_text(doc(para(text("Assigning to "), text("Hristo", marks=[])))) == (
        "Assigning to Hristo"
    )


def test_marks_do_not_affect_the_text():
    bold = text("important", marks=[{"type": "strong"}])
    assert adf_to_text(doc(para(text("this is "), bold))) == "this is important"


def test_paragraphs_are_newline_separated():
    assert (
        adf_to_text(doc(para(text("first")), para(text("second")))) == "first\nsecond"
    )


def test_hard_break_becomes_a_newline():
    body = doc(para(text("line one"), {"type": "hardBreak"}, text("line two")))
    assert adf_to_text(body) == "line one\nline two"


def test_mention_uses_its_display_text():
    mention = {
        "type": "mention",
        "attrs": {"id": "557058:f58131cb", "text": "@William Durand"},
    }
    assert adf_to_text(doc(para(text("cc "), mention))) == "cc @William Durand"


def test_mention_without_text_attr_does_not_crash():
    mention = {"type": "mention", "attrs": {"id": "557058:f58131cb"}}
    assert adf_to_text(doc(para(text("cc "), mention))) == "cc"


def test_inline_card_uses_its_url():
    card = {"type": "inlineCard", "attrs": {"url": "https://example.com/x"}}
    assert adf_to_text(doc(para(text("see "), card))) == "see https://example.com/x"


def test_emoji_and_status_use_their_text():
    emoji = {"type": "emoji", "attrs": {"shortName": ":check:", "text": "✅"}}
    status = {"type": "status", "attrs": {"text": "DONE", "color": "green"}}
    assert adf_to_text(doc(para(emoji, text(" "), status))) == "✅ DONE"


def test_code_block_keeps_its_contents():
    block = {
        "type": "codeBlock",
        "attrs": {"language": "sql"},
        "content": [text("SELECT 1")],
    }
    assert adf_to_text(doc(para(text("try:")), block)) == "try:\nSELECT 1"


def test_bullet_list_items_land_on_separate_lines():
    def item(value):
        return {"type": "listItem", "content": [para(text(value))]}

    body = doc({"type": "bulletList", "content": [item("first"), item("second")]})
    assert adf_to_text(body) == "first\nsecond"


def test_heading_and_blockquote_are_blocks():
    heading = {"type": "heading", "attrs": {"level": 2}, "content": [text("Summary")]}
    quote = {"type": "blockquote", "content": [para(text("quoted"))]}
    assert adf_to_text(doc(heading, quote, para(text("after")))) == (
        "Summary\nquoted\nafter"
    )


def test_table_cells_are_flattened():
    def cell(value):
        return {"type": "tableCell", "content": [para(text(value))]}

    row = {"type": "tableRow", "content": [cell("a"), cell("b")]}
    body = doc({"type": "table", "content": [row]})
    assert adf_to_text(body) == "a\nb"


def test_unknown_node_types_are_skipped_without_crashing():
    """Jira adds node types over time; an unrecognised one must not fail the run."""
    body = doc(
        para(text("before")),
        {"type": "someFutureNodeType", "attrs": {"whatever": 1}},
        para(text("after")),
    )
    assert adf_to_text(body) == "before\nafter"


def test_unknown_node_type_with_content_still_yields_its_text():
    body = doc({"type": "someFutureWrapper", "content": [para(text("inner"))]})
    assert adf_to_text(body) == "inner"


def test_media_is_dropped():
    """Documented loss: images have no text to contribute."""
    media = {
        "type": "mediaSingle",
        "content": [{"type": "media", "attrs": {"id": "abc", "type": "file"}}],
    }
    assert adf_to_text(doc(para(text("screenshot:")), media)) == "screenshot:"


def test_blank_lines_are_collapsed_and_ends_stripped():
    body = doc(para(), para(text("  content  ")), para(), para())
    assert adf_to_text(body) == "content"


def test_deeply_nested_content_is_reached():
    inner = para(text("deep"))
    for _ in range(10):
        inner = {"type": "blockquote", "content": [inner]}
    assert adf_to_text(doc(inner)) == "deep"


def test_non_dict_content_entries_are_ignored():
    assert (
        adf_to_text({"type": "doc", "content": ["junk", None, para(text("ok"))]})
        == "ok"
    )


@pytest.mark.parametrize("body", [[], 0, False, 1.5])
def test_unexpected_body_types_return_empty_string(body):
    assert adf_to_text(body) == ""


def test_real_srein_comment_with_mentions_and_a_bullet_list():
    """Captured verbatim from SREIN-1603 comment 1565444 via the v3 API."""
    body = {
        "type": "doc",
        "version": 1,
        "content": [
            {
                "type": "paragraph",
                "content": [
                    text("Hi "),
                    {
                        "type": "mention",
                        "attrs": {
                            "id": "712020:482b",
                            "text": "@Bhee Persaud",
                            "accessLevel": "",
                        },
                    },
                    text(" , we’re on it!"),
                ],
                "attrs": {"localId": "dc50949aec82"},
            },
            {
                "type": "bulletList",
                "content": [
                    {
                        "type": "listItem",
                        "attrs": {"localId": "5646e80a8c37"},
                        "content": [
                            para(
                                {
                                    "type": "mention",
                                    "attrs": {"id": "6159", "text": "@Graham Beckley"},
                                },
                                text(
                                    " please help with this, and file a separate ticket for argo cleanup."
                                ),
                            )
                        ],
                    },
                    {
                        "type": "listItem",
                        "content": [
                            para(
                                text(
                                    "We’ve set this to P1 (current default priority level)"
                                )
                            )
                        ],
                    },
                    {
                        "type": "listItem",
                        "content": [para(text("No action needed from requestor."))],
                    },
                ],
            },
        ],
    }

    assert adf_to_text(body) == (
        "Hi @Bhee Persaud , we’re on it!\n"
        "@Graham Beckley please help with this, and file a separate ticket for argo cleanup.\n"
        "We’ve set this to P1 (current default priority level)\n"
        "No action needed from requestor."
    )


def test_real_srein_comment_with_code_marks_and_an_inline_card():
    """Captured verbatim from SREIN-1603 comments 1578236 and 1578294.

    Documents a real loss: a `link` mark's href is dropped and only its display
    text survives, so "label" here loses the URL it pointed at.
    """
    coded = {
        "type": "doc",
        "version": 1,
        "content": [
            para(
                text("Selectively synced "),
                text("argocd-webservices", marks=[{"type": "code"}]),
                text(
                    " to remove those three applications. They no longer appear under the "
                ),
                text("tenant=iam", marks=[{"type": "code"}]),
                text(" "),
                text(
                    "label",
                    marks=[
                        {"type": "link", "attrs": {"href": "https://example.com/q"}}
                    ],
                ),
                text("."),
            )
        ],
    }
    assert adf_to_text(coded) == (
        "Selectively synced argocd-webservices to remove those three applications. "
        "They no longer appear under the tenant=iam label."
    )

    quoted = {
        "type": "doc",
        "version": 1,
        "content": [
            para(
                text("> file a separate ticket for argo cleanup"),
                {"type": "hardBreak"},
                {"type": "hardBreak"},
                text("Filed "),
                {
                    "type": "inlineCard",
                    "attrs": {
                        "url": "https://mozilla-hub.atlassian.net/browse/MZCLD-3592"
                    },
                },
                text(" "),
            )
        ],
    }
    assert adf_to_text(quoted) == (
        "> file a separate ticket for argo cleanup\n"
        "Filed https://mozilla-hub.atlassian.net/browse/MZCLD-3592"
    )
