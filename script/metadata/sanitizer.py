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

Usage:
    s = Sanitizer(project="my-gcp-project")
    r = s.sanitize("contact jane@example.com")
    # r.action == "mask", r.value == "contact [EMAIL_ADDRESS]"

DLP detection is imperfect - it misses some values and over-flags others - so
this is a best-effort scrub, not a guarantee. The infoType lists below are a
starting point and should be tuned against real data.

The classifier currently calls this in memory at prompt-build time, leaving the
stored profiling values untouched; a later step can reuse the same module to
overwrite the stored values once the policy is settled.
"""

import logging
from dataclasses import dataclass

from google.cloud import dlp_v2

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


@dataclass
class SanitizeResult:
    """Outcome of sanitizing one value.

    action     - KEEP, MASK, or DROP.
    value      - the sanitized string (None when dropped).
    info_types - DLP infoType names that fired (for the raw-vs-clean report).
    """

    action: str
    value: "str | None"
    info_types: list


def _make_client(quota_project):
    """DLP client, optionally pinning the billing/quota project.

    Application-default credentials may carry a quota project that lacks the DLP
    API; passing quota_project re-points it to a project where DLP is enabled and
    the caller has serviceusage + dlp.user (mirrors the prototype setup).
    """
    if quota_project:
        import google.auth

        creds, _ = google.auth.default(quota_project_id=quota_project)
        return dlp_v2.DlpServiceClient(credentials=creds)
    return dlp_v2.DlpServiceClient()


class Sanitizer:
    """Sanitizes sample strings via a single DLP deidentify_content per value."""

    def __init__(
        self, project, location="global", quota_project=None, min_likelihood="POSSIBLE"
    ):
        """Build the client and resolve the active infoType set.

        Configured infoTypes are intersected with what DLP actually supports, so
        an unknown name in the draft sets is skipped (with a warning) rather than
        erroring the whole request.
        """
        self._client = _make_client(quota_project)
        self.parent = f"projects/{project}/locations/{location}"
        self.min_likelihood = dlp_v2.Likelihood[min_likelihood]

        supported = {it.name for it in self._client.list_info_types().info_types}
        self.mask = sorted(TIER1_MASK_INFOTYPES & supported)
        self.drop = sorted(TIER2_DROP_INFOTYPES & supported)
        skipped = (TIER1_MASK_INFOTYPES | TIER2_DROP_INFOTYPES) - supported
        if skipped:
            logging.warning(
                "Sanitizer: %d configured infoTypes unsupported by DLP, skipping: %s",
                len(skipped),
                ", ".join(sorted(skipped)),
            )
        self._active = [{"name": n} for n in self.mask + self.drop]
        self._deidentify_config = self._build_deidentify_config()
        self._cache = {}

    def _build_deidentify_config(self):
        """Mask tier-1 spans with the infoType name; replace tier-2 with sentinel."""
        transformations = []
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
    def _fired_info_types(resp):
        """Return infoType names with a non-zero transformation count."""
        fired = []
        overview = getattr(resp, "overview", None)
        if not overview:
            return fired
        for summary in overview.transformation_summaries:
            count = sum(r.count for r in summary.results)
            if count > 0 and summary.info_type.name:
                fired.append(summary.info_type.name)
        return fired

    def sanitize(self, value):
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

    def sanitize_many(self, values):
        """Sanitize an iterable of strings; return {value: SanitizeResult}, deduped."""
        out = {}
        for v in values:
            if v is not None and v not in out:
                out[v] = self.sanitize(v)
        return out


def _main():
    """Ad-hoc check: sanitize values from argv (or stdin) and print the verdicts."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="DLP parent project")
    parser.add_argument(
        "--quota-project", help="override the ADC billing/quota project"
    )
    parser.add_argument("values", nargs="*", help="values to sanitize (else stdin)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    s = Sanitizer(project=args.project, quota_project=args.quota_project)
    logging.info("Active mask=%s drop=%s", s.mask, s.drop)

    values = args.values or [ln.rstrip("\n") for ln in sys.stdin]
    for v, res in s.sanitize_many(values).items():
        print(f"{res.action.upper():5} {v!r} -> {res.value!r}  {res.info_types}")


if __name__ == "__main__":
    _main()
