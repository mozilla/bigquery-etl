"""Preprocess the Mozilla data taxonomy CSV into a clean JSON file for the classifier.

Reads `Taxonomy overview - Data Types.csv`, normalizes labels, fixes known typos,
and emits a flat list of {label, parent, display_name, description, examples}
as `taxonomy.json`.
"""

import csv
import json
import re
from pathlib import Path

HERE = Path(__file__).parent
CSV_PATH = HERE / "Taxonomy overview - Data Types.csv"
JSON_PATH = HERE / "taxonomy.json"

# Known typos in the CSV → canonical label.
LABEL_FIXES = {
    "user.behaviour.media_consumption": "user.behavior.media_consumption",
    "user.behaviour.search.term": "user.behavior.search.term",
    "personnel.demographic.Marital_status_orientation": "personnel.demographic.sexual_orientation",
    "personnel.human_resouces.operations": "personnel.human_resources.operations",
}

# Top-level subject headers (column 0) → synthesized label.
SUBJECT_LABELS = {
    "System": "system",
    "User": "user",
    "company": "company",
    "Personnel": "personnel",
    "Job applicants": "jobapplicants",
    "Other": "other",
}


def canonical_label(raw):
    """Strip whitespace (including internal), apply known typo fixes, lowercase."""
    label = re.sub(r"\s+", "", raw).rstrip(".")
    # Keep casing as-is for the lookup, then return fixed version if matched
    if label in LABEL_FIXES:
        return LABEL_FIXES[label]
    return label.lower()


def parent_of(label):
    """Return dotted parent, or None for top-level labels."""
    if "." not in label:
        return None
    return label.rsplit(".", 1)[0]


def parse_row(row):
    """Return a taxonomy entry or None for blank/unusable rows."""
    padded = (row + [""] * 7)[:7]
    subject, level1, level2, name, description, examples, _ = padded

    if level2.strip():
        label = canonical_label(level2)
    elif level1.strip():
        label = canonical_label(level1)
    elif subject.strip() in SUBJECT_LABELS:
        label = SUBJECT_LABELS[subject.strip()]
    else:
        return None

    return {
        "label": label,
        "parent": parent_of(label),
        "display_name": name.strip() or None,
        "description": description.strip() or None,
        "examples": examples.strip() or None,
    }


def main():
    rows = []
    seen = set()
    with open(CSV_PATH, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # drop header
        for raw_row in reader:
            entry = parse_row(raw_row)
            if entry is None:
                continue
            if entry["label"] in seen:
                continue
            seen.add(entry["label"])
            rows.append(entry)

    JSON_PATH.write_text(json.dumps(rows, indent=2) + "\n")
    print(f"Wrote {len(rows)} taxonomy entries → {JSON_PATH}")


if __name__ == "__main__":
    main()
