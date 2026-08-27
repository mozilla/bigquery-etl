"""The Mozilla data taxonomy the classifier labels columns against.

The version lives in the filename. A new taxonomy is a new file
(`taxonomy_v2.json`) alongside this one, so classifications stay tied to the
taxonomy that produced them, and `taxonomy_v1.json` is never edited in place.
"""

import json
import re
from typing import Any

from .config import TAXONOMY_PATH

TAXONOMY_VERSION_RE = re.compile(r"^taxonomy_(v\d+)$")


def load_taxonomy() -> list[dict[str, Any]]:
    """Load and validate the committed taxonomy.

    Raises ValueError if the taxonomy is empty, an entry has no label, or a
    label appears more than once. The file is hand-maintained, so a bad edit
    should fail here rather than silently drop or duplicate a prompt label.
    """
    taxonomy = json.loads(TAXONOMY_PATH.read_text())
    if not taxonomy:
        raise ValueError(f"taxonomy {TAXONOMY_PATH} is empty")

    seen: set[str] = set()
    for entry in taxonomy:
        label = entry.get("label")
        if not label:
            raise ValueError(f"taxonomy entry without a label: {entry!r}")
        if label in seen:
            raise ValueError(f"duplicate taxonomy label: {label}")
        seen.add(label)

    return taxonomy


def taxonomy_version() -> str:
    """Return the taxonomy version, e.g. "v1", derived from the filename."""
    match = TAXONOMY_VERSION_RE.match(TAXONOMY_PATH.stem)
    if match is None:
        raise ValueError(
            f"taxonomy filename {TAXONOMY_PATH.name} does not match taxonomy_v<digits>"
        )
    return match.group(1)


def taxonomy_prompt_block(taxonomy: list[dict[str, Any]]) -> str:
    """Compact the taxonomy into a single JSON block for the prompt."""
    compact = [
        {
            "label": e["label"],
            "name": e.get("display_name") or "",
            "desc": e.get("description") or "",
            "examples": e.get("examples") or "",
        }
        for e in taxonomy
    ]
    return json.dumps(compact, separators=(",", ":"))
