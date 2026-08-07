"""Build the classification prompt for one column.

Renders the column dict as the pipeline hands it over, already sanitized: this
module does not scrub sample values.
"""

import re
from typing import Any

TOP_N_PROBES = 3

_NON_ALPHANUMERIC_RE = re.compile(r"[^a-z0-9]")


def _normalize_name(name: str | None) -> str:
    """Strip all non-alphanumeric characters and lowercase for fuzzy matching."""
    return _NON_ALPHANUMERIC_RE.sub("", (name or "").lower())


def find_matching_probes(
    column_name: str, probes: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Return up to TOP_N_PROBES candidate probes for a column name.

    The fuzzy matcher for ordinary profiled columns. Glean metric leaves use
    find_metric_probes instead: fuzzy matching would pair them with the wrong
    probe rather than with none.
    """
    col_norm = _normalize_name(column_name)
    scored = []
    for probe in probes:
        pname = probe.get("probe_name") or ""
        if not pname:
            continue
        pname_norm = _normalize_name(pname)
        if col_norm == pname_norm:
            score = 3
        elif pname_norm.endswith(col_norm) or pname_norm.startswith(col_norm):
            score = 2
        elif col_norm in pname_norm or pname_norm in col_norm:
            score = 1
        else:
            continue
        scored.append((score, probe))
    scored.sort(key=lambda x: -x[0])
    return [p for _, p in scored[:TOP_N_PROBES]]


def find_metric_probes(
    column_name: str, probes: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Pair a Glean metric leaf column with its product probe by exact name.

    A Glean scalar metric ``category.name`` materializes in BigQuery as
    ``metrics.<type>.<category>_<name>``, whose leaf segment normalizes to
    exactly the dotted probe name (normalize('account_user_id') ==
    normalize('account.user_id')). Matching on that exact normalized name -
    rather than the fuzzy substring match used for arbitrary profiled columns -
    maps each metric to its own probe (so account_user_id_sha256 pairs with
    account.user_id_sha256, not account.user_id) and prevents short generic
    leaves from matching unrelated probes.

    Returns up to TOP_N_PROBES probes, or an empty list when the leaf has no
    exact match.
    """
    leaf_norm = _normalize_name(column_name.rsplit(".", 1)[-1])
    matched = []
    for probe in probes:
        pname = probe.get("probe_name") or ""
        if pname and _normalize_name(pname) == leaf_norm:
            matched.append(probe)
            if len(matched) == TOP_N_PROBES:
                break
    return matched


def _profile_block(column: dict[str, Any]) -> str:
    """Render the observed profiling signals for one column as prompt text."""
    lines = []
    if column.get("is_glean_metric"):
        return (
            "Glean metric column - not data-profiled; classify from the metric"
            " definition and its data_sensitivity."
        )
    if column.get("column_tier") == "pii_suppressed":
        lines.append(
            "PII-suppressed: column name matched a known PII pattern, so it was"
            " not scanned. Treat as personal data."
        )
    null_rate = column.get("null_rate")
    if null_rate is not None:
        lines.append(f"Null rate: {null_rate}%")
    distinct_count = column.get("distinct_count")
    if distinct_count is not None:
        hc = " (high cardinality)" if column.get("is_high_cardinality") else ""
        lines.append(f"Distinct values: {distinct_count}{hc}")
    values = column.get("values") or []
    if values:
        shown = ", ".join(f"{str(v)!r} ({f:,})" for v, f in values[:10])
        lines.append(f"Top values (value: frequency): {shown}")
    elif column.get("example_value") is not None:
        lines.append(f"Example value: {column['example_value']}")
    return "\n".join(lines) if lines else "No profiling stats available."


def _probes_block(probes: list[dict[str, Any]]) -> str:
    """Render the candidate probes matched to the column name."""
    if not probes:
        return "Candidate probes: none matched."
    probe_lines = []
    for p in probes:
        parts = [f"name={p['probe_name']}"]
        if p.get("probe_type"):
            parts.append(f"type={p['probe_type']}")
        if p.get("data_sensitivity"):
            parts.append(f"data_sensitivity={p['data_sensitivity']}")
        if p.get("tags"):
            parts.append(f"tags={p['tags']}")
        if p.get("probe_description"):
            parts.append(f"desc={p['probe_description']}")
        probe_lines.append("  - " + " | ".join(parts))
    return "Candidate probes:\n" + "\n".join(probe_lines)


def build_classification_prompt(
    column: dict[str, Any],
    table: str,
    probes: list[dict[str, Any]],
    taxonomy_json: str,
) -> str:
    """Build a prompt asking the LLM to assign a taxonomy label to one column.

    ``table`` is ``dataset.table``; the project is constant across a run and is
    not worth the tokens. ``taxonomy_json`` is ``taxonomy_prompt_block``'s
    output, rendered once per run by the caller.
    """
    existing_desc = column.get("bq_description")
    desc_line = (
        f"Existing column description (from BigQuery schema): {existing_desc}\n"
        if existing_desc
        else ""
    )

    return (
        "You are classifying a BigQuery column against Mozilla's data taxonomy.\n\n"
        f"Table: {table}\n"
        f"Column: {column['column_name']}\n"
        f"Data type: {column['data_type']}\n"
        f"{desc_line}"
        f"Profiled data:\n{_profile_block(column)}\n\n"
        f"{_probes_block(probes)}\n\n"
        "Taxonomy (JSON list of {label, name, desc, examples}):\n"
        f"{taxonomy_json}\n\n"
        "Pick the single most specific taxonomy label that fits the column's own"
        " meaning. Do not choose a label more specific than the column itself"
        " warrants just because sibling columns or the table are more specific: a"
        " timestamp, boolean, or generic surrogate key on an identifier-bearing"
        " table is not itself that identifier. Identifier leaves (e.g. *.fxid,"
        " *.client_id, user.unique_id) apply only when the column value IS that"
        " identifier; otherwise prefer the parent category (e.g. user.account)."
        " Concretely, a column literally named `id` is the table's own row key, not"
        " the Firefox Account identifier: use user.account.fxid only for uid/userId,"
        " and when a table has both `id` and `uid`/`userId`, classify `id` as"
        " user.unique_id. If"
        " multiple labels apply, list the extras in secondary_labels. Use the Glean"
        " data_sensitivity signal to disambiguate when present (e.g. highly_sensitive"
        " strongly implies user.behavior, user.content, user.location.precise,"
        " etc.)."
    )
