"""Flatten Atlassian Document Format into plain text.

Jira REST API v3 returns rich text fields - comment bodies in particular - as an
ADF document rather than a string. This module reduces one to readable text for
storage in a single BigQuery STRING column.

The reduction is deliberately lossy and forgiving:

- Formatting marks (bold, links, colours) are dropped; only the characters
  survive.
- Nodes that carry their text in `attrs` rather than a child `text` node -
  mentions, emoji, status lozenges, smart links - contribute that attribute.
- Nodes with no textual content, such as embedded images, contribute nothing.
- Unrecognised node types are traversed for children and otherwise skipped, so a
  node type Jira introduces later degrades to missing text rather than a failed
  ETL run.
"""

from typing import Any

# Nodes that should end with a line break, so block structure survives as lines.
BLOCK_TYPES = frozenset(
    {
        "blockquote",
        "bulletList",
        "codeBlock",
        "decisionItem",
        "decisionList",
        "expand",
        "heading",
        "listItem",
        "mediaGroup",
        "mediaSingle",
        "orderedList",
        "panel",
        "paragraph",
        "rule",
        "table",
        "tableCell",
        "tableHeader",
        "tableRow",
        "taskItem",
        "taskList",
    }
)

# Nodes whose text lives in an `attrs` entry rather than in child `text` nodes.
ATTR_TEXT_KEYS = {
    "blockCard": "url",
    "date": "timestamp",
    "emoji": "text",
    "inlineCard": "url",
    "mention": "text",
    "placeholder": "text",
    "status": "text",
}


def _flatten(node: Any) -> str:
    """Reduce one ADF node and its descendants to text, with block line breaks."""
    if not isinstance(node, dict):
        return ""

    node_type = str(node.get("type") or "")

    if node_type == "text":
        return node.get("text") or ""

    if node_type == "hardBreak":
        return "\n"

    attr_key = ATTR_TEXT_KEYS.get(node_type)
    if attr_key is not None:
        value = (node.get("attrs") or {}).get(attr_key)
        if value is not None:
            return str(value)

    content = node.get("content")
    inner = (
        "".join(_flatten(child) for child in content)
        if isinstance(content, list)
        else ""
    )

    if node_type in BLOCK_TYPES:
        return f"{inner}\n"

    return inner


def adf_to_text(body: Any) -> str:
    """Flatten an ADF document to plain text.

    Accepts a string unchanged, so a caller does not have to know whether a given
    payload came back as ADF or as an already-plain body. Anything else that is not
    an ADF document reduces to an empty string.
    """
    if isinstance(body, str):
        return body.strip()

    text = _flatten(body)

    # Blank lines are dropped rather than preserved. Nested blocks each contribute
    # a line break - a list item wraps a paragraph, a table cell wraps a paragraph -
    # so blank lines reflect ADF nesting depth more than authorial intent, and
    # keeping them would make the output unpredictable. One line per block is a rule
    # a consumer can rely on.
    return "\n".join(line for line in (ln.strip() for ln in text.split("\n")) if line)
