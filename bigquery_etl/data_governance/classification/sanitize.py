"""Scrub PII and sensitive data out of sample values before they go to an LLM.

The column classifier sends example values from each column to an external LLM.
Some values may contain personal or sensitive data, so this module runs each one
through Google Cloud DLP (Sensitive Data Protection) and either masks or removes
what DLP finds.

Two actions, by what DLP detects:
  - MASK - replace only the matched span. For self-identifying personal data
    (email, phone, IP, person name, ...); masking is enough to minimize it.
  - DROP - discard the whole value. For special categories (SSN, financial,
    health, precise location, ...); these we remove entirely rather than partly
    redact, so any single hit voids the value.
  - Anything else DLP might flag (dates, times, coarse city/country) is not in
    our active detector set, so it is left untouched.
"""

from dataclasses import dataclass
from typing import Any, Iterable

import google.auth
from google.cloud import dlp_v2

from .config import ClassificationConfig

DEFAULT_MIN_LIKELIHOOD = "POSSIBLE"

# Tier-1: self-identifying personal data. We mask the matched span rather than
# drop the value (masking is sufficient minimization for these).
TIER1_MASK_INFOTYPES = {
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "IP_ADDRESS",
    "PERSON_NAME",
    "STREET_ADDRESS",
    "DATE_OF_BIRTH",
}

# Tier-2: special categories we aim to keep out of AI prompts. Drop the entire
# value (remove entirely rather than partially redact).
TIER2_DROP_INFOTYPES = {
    "US_SOCIAL_SECURITY_NUMBER",
    "US_INDIVIDUAL_TAXPAYER_IDENTIFICATION_NUMBER",
    "PASSPORT",
    "CREDIT_CARD_NUMBER",
    "IBAN_CODE",
    "SWIFT_CODE",
    "US_BANK_ROUTING_MICR",
    "US_HEALTHCARE_NPI",
    "US_DEA_NUMBER",
    "MEDICAL_TERM",
    "ICD9_CODE",
    "ICD10_CODE",
    "LATITUDE_LONGITUDE",
    "ETHNIC_GROUP",
    "RELIGIOUS_TERM",
}

# Replaces a dropped (tier-2) value in the deidentified text; its presence after
# deidentify is a robust signal that a special category fired -> drop the value.
_DROP_SENTINEL = "[SPECIAL_CATEGORY_REMOVED]"

KEEP, MASK, DROP = "keep", "mask", "drop"


@dataclass(frozen=True)
class SanitizeResult:
    """Outcome of sanitizing one value.

    action     - KEEP, MASK, or DROP.
    value      - the sanitized string (None when dropped).
    info_types - DLP infoType names that fired.
    """

    action: str
    value: str | None
    info_types: list[str]


def _make_client(quota_project: str | None) -> dlp_v2.DlpServiceClient:
    """DLP client, optionally pinning the billing/quota project.

    Application-default credentials may carry a quota project that lacks the DLP
    API; passing quota_project re-points it to a project where DLP is enabled and
    the caller has serviceusage + dlp.user.
    """
    if quota_project:
        creds, _ = google.auth.default(quota_project_id=quota_project)
        return dlp_v2.DlpServiceClient(credentials=creds)
    return dlp_v2.DlpServiceClient()


class Sanitizer:
    """Sanitizes sample strings via a single DLP deidentify_content per value."""

    def __init__(
        self,
        project: str,
        quota_project: str | None = None,
        min_likelihood: str = DEFAULT_MIN_LIKELIHOOD,
        client: Any = None,
    ):
        """Build the client and resolve the active infoType set.

        Raises if DLP does not support every configured infoType, before any
        value has been sent anywhere. Pass client to inject an alternative to the
        default DLP client.
        """
        self._client = client if client is not None else _make_client(quota_project)
        # Global only, and deliberately not configurable: which infoTypes exist
        # varies by location, so a regional parent would also need the
        # list_info_types call below scoped to that location.
        self.parent = f"projects/{project}/locations/global"
        # proto-plus enums are indexable by name, but mypy does not see them as enums
        self.min_likelihood = dlp_v2.Likelihood[min_likelihood]  # type: ignore[misc]

        supported = {it.name for it in self._client.list_info_types().info_types}
        missing = (TIER1_MASK_INFOTYPES | TIER2_DROP_INFOTYPES) - supported
        if missing:
            # Scrubbing is the only thing keeping raw sample values out of the
            # prompt, so a detector DLP will not install is a reason to stop
            # rather than to carry on with a smaller set.
            raise ValueError(
                "DLP does not support these configured infoTypes: "
                + ", ".join(sorted(missing))
            )
        self.mask = sorted(TIER1_MASK_INFOTYPES)
        self.drop = sorted(TIER2_DROP_INFOTYPES)
        self._active = [{"name": n} for n in self.mask + self.drop]
        self._deidentify_config = self._build_deidentify_config()
        self._cache: dict[str, SanitizeResult] = {}

    def _build_deidentify_config(self) -> dict[str, Any]:
        """Mask tier-1 spans with the infoType name; replace tier-2 with sentinel."""
        transformations: list[dict[str, Any]] = []
        if self.mask:
            transformations.append(
                {
                    "info_types": [{"name": n} for n in self.mask],
                    "primitive_transformation": {"replace_with_info_type_config": {}},
                }
            )
        if self.drop:
            transformations.append(
                {
                    "info_types": [{"name": n} for n in self.drop],
                    "primitive_transformation": {
                        "replace_config": {
                            "new_value": {"string_value": _DROP_SENTINEL}
                        }
                    },
                }
            )
        return {"info_type_transformations": {"transformations": transformations}}

    @staticmethod
    def _fired_info_types(resp: Any) -> list[str]:
        """Return infoType names with a non-zero transformation count."""
        fired: list[str] = []
        overview = getattr(resp, "overview", None)
        if not overview:
            return fired
        for summary in overview.transformation_summaries:
            count = sum(r.count for r in summary.results)
            if count > 0 and summary.info_type.name:
                fired.append(summary.info_type.name)
        return fired

    def sanitize(self, value: str | None) -> SanitizeResult:
        """Sanitize one string, returning a SanitizeResult (cached per value)."""
        if value is None:
            return SanitizeResult(KEEP, None, [])
        if not value.strip() or not self._active:
            return SanitizeResult(KEEP, value, [])
        if value in self._cache:
            return self._cache[value]

        resp = self._client.deidentify_content(
            request={
                "parent": self.parent,
                "deidentify_config": self._deidentify_config,
                "inspect_config": {
                    "info_types": self._active,
                    "min_likelihood": self.min_likelihood,
                },
                "item": {"value": value},
            }
        )
        deidentified = resp.item.value
        fired = self._fired_info_types(resp)

        if _DROP_SENTINEL in deidentified or set(fired) & set(self.drop):
            result = SanitizeResult(DROP, None, fired)
        elif deidentified != value or set(fired) & set(self.mask):
            result = SanitizeResult(MASK, deidentified, fired)
        else:
            result = SanitizeResult(KEEP, value, fired)

        self._cache[value] = result
        return result

    def sanitize_many(self, values: Iterable[str | None]) -> dict[str, SanitizeResult]:
        """Sanitize an iterable of strings; return {value: SanitizeResult}, deduped."""
        out: dict[str, SanitizeResult] = {}
        for v in values:
            if v is not None and v not in out:
                out[v] = self.sanitize(v)
        return out


def sanitizer_for(config: ClassificationConfig) -> Sanitizer:
    """Build the Sanitizer for a run."""
    return Sanitizer(
        project=config.dlp_project or config.project,
        quota_project=config.dlp_quota_project,
    )


def suppress_high_cardinality_samples(columns: list[dict[str, Any]]) -> int:
    """Drop example/top values for high-cardinality columns, in place.

    High-cardinality columns (distinct_count over the profiler threshold) are
    almost always identifiers/tokens/hashes: their top-N values carry no aggregate
    signal (each occurs ~once) and are the values most likely to be a sensitive or
    opaque identifier that DLP cannot detect (uid, session tokens, Stripe ids).
    Keep the stats - distinct_count and the is_high_cardinality flag, which are the
    signal the classifier actually uses - and drop the raw values. Mutates the
    column dicts in place and returns the number of columns suppressed.
    """
    n = 0
    for c in columns:
        if c.get("is_high_cardinality") and (c.get("example_value") or c.get("values")):
            c["example_value"] = None
            c["values"] = []
            n += 1
    return n


def _change_record(column: str, field: str, res: SanitizeResult) -> dict[str, Any]:
    """Describe one sanitized value, without the value itself.

    Deliberately holds nothing that identifies what DLP flagged: the only place
    these records go is the run log.
    """
    return {
        "column": column,
        "field": field,
        "action": res.action,
        "info_types": res.info_types,
    }


def sanitize_columns(
    sanitizer: Sanitizer, columns: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """DLP-scrub each column's example_value / top values, in place.

    Non-destructive to the profiling table: only the in-memory column dicts are
    mutated, so the classifier prompts on clean values while the stored table
    keeps the originals for re-testing. Tier-1 hits are masked; tier-2 hits drop
    the value (and, for top values, the whole entry). Returns one record per
    changed value, holding no value that identifies what DLP flagged.
    """
    strings = []
    for c in columns:
        if c.get("example_value"):
            strings.append(c["example_value"])
        for v, _f in c.get("values") or []:
            if v is not None:
                strings.append(v)
    if not strings:
        return []

    results = sanitizer.sanitize_many(strings)
    changes = []
    for c in columns:
        col = c["column_name"]
        ex = c.get("example_value")
        if ex:
            res = results.get(ex)
            if res and res.action != KEEP:
                c["example_value"] = res.value
                changes.append(_change_record(col, "example_value", res))
        if "values" in c:
            kept = []
            for v, f in c["values"] or []:
                res = results.get(v)
                if res is None or res.action == KEEP:
                    kept.append((v, f))
                    continue
                changes.append(_change_record(col, "values", res))
                if res.action == MASK:
                    kept.append((res.value, f))
            c["values"] = kept
    return changes
