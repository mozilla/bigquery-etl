"""Fetch Jira issues and load them into BigQuery.

Every table gets the columns in `OUTPUT_SCHEMA`. A table that needs more can
declare `JiraField` specs and pass them to `JiraIssueBigQueryIntegration.run`;
they become additional columns appended after the base ones. Tables that declare
nothing are unaffected, which is what keeps the pre-existing tables working.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from google.cloud import bigquery

from .client import JiraClient

OUTPUT_SCHEMA = [
    bigquery.SchemaField("issue_key", "STRING"),
    bigquery.SchemaField("project_key", "STRING"),
    bigquery.SchemaField("issue_type", "STRING"),
    bigquery.SchemaField("summary", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("created", "TIMESTAMP"),
    bigquery.SchemaField("resolved", "TIMESTAMP"),
    bigquery.SchemaField("updated", "TIMESTAMP"),
]

REQUEST_FIELDS = [
    "summary",
    "status",
    "issuetype",
    "project",
    "created",
    "resolutiondate",
    "updated",
]

# Keys Jira nests a human-readable value under, most specific first. `name`
# covers priority/status/resolution/issuetype, `displayName` covers users,
# `value` covers single-select customfields.
VALUE_KEYS = ("name", "displayName", "value")

VALID_MODES = ("NULLABLE", "REPEATED")

# Types this module knows how to produce. bigquery.SchemaField accepts any
# string, and _extra_values falls through to the untouched-value branch for
# anything unrecognised, so a typo like "DATETIEM" would build fine and only
# surface as a load failure.
VALID_TYPES = (
    "STRING",
    "BOOL",
    "INT64",
    "FLOAT64",
    "NUMERIC",
    "DATE",
    "DATETIME",
    "TIMESTAMP",
)

# Types whose extraction yields a scalar or None, never a list, so pairing them
# with REPEATED would produce a permanently empty column.
SCALAR_ONLY_TYPES = ("DATE", "DATETIME", "TIMESTAMP")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JiraField:
    """An additional Jira field to project onto its own BigQuery column.

    `column` is the BigQuery column name, `jira_field` the Jira field id as the
    API names it (`priority`, `labels`, `customfield_10420`). `bq_type` of
    TIMESTAMP routes the value through the same UTC normalization the base
    timestamps use, which requires an offset in the source; set `utc_naive` for a
    zone-less source that the data model specifies as UTC. `mode="REPEATED"` is
    for Jira fields that return an array, such as `labels` or `components`.
    """

    column: str
    jira_field: str
    bq_type: str = "STRING"
    mode: str = "NULLABLE"
    utc_naive: bool = False

    def __post_init__(self) -> None:
        """Reject a mode BigQuery would not accept, at declaration rather than load."""
        if self.mode not in VALID_MODES:
            raise ValueError(
                f"JiraField({self.column!r}) has mode {self.mode!r}; "
                f"expected one of {VALID_MODES}"
            )
        if self.bq_type not in VALID_TYPES:
            raise ValueError(
                f"JiraField({self.column!r}) has bq_type {self.bq_type!r}; "
                f"expected one of {VALID_TYPES}"
            )
        if self.utc_naive and self.bq_type != "TIMESTAMP":
            raise ValueError(
                f"JiraField({self.column!r}) sets utc_naive with bq_type "
                f"{self.bq_type!r}; it only applies to TIMESTAMP, whose parser is "
                "the one that needs an offset"
            )
        if self.mode == "REPEATED" and self.bq_type in SCALAR_ONLY_TYPES:
            raise ValueError(
                f"JiraField({self.column!r}) pairs mode REPEATED with bq_type "
                f"{self.bq_type!r}, whose parser always returns a scalar or None, "
                "so the column would always be empty"
            )


def extract_value(raw: Any) -> Any:
    """Reduce a raw Jira field value to something a scalar column can hold.

    Jira returns bare scalars for text and numeric fields, objects for anything
    that references an entity, and arrays for multi-valued fields. An object
    whose shape is not recognised yields None rather than the dict itself,
    because a dict would fail the column's type at load time and take the whole
    run down with it.
    """
    if isinstance(raw, list):
        # Unusable elements are dropped rather than kept as None: the only
        # consumer of a list is a REPEATED column, and BigQuery rejects a null
        # element inside one with "Array specifies a null element", which would
        # fail the load for the same reason the scalar branch returns None.
        return [value for value in (extract_value(i) for i in raw) if value is not None]

    if isinstance(raw, dict):
        for key in VALUE_KEYS:
            if raw.get(key) is not None:
                return raw[key]
        return None

    return raw


def parse_datetime(raw: Any) -> Optional[str]:
    """Normalize a zone-less Jira datetime string to a BigQuery DATETIME literal.

    Some Jira custom fields are plain text holding a wall-clock time with no
    timezone and often no seconds, e.g. the IIM incident timing fields, which
    arrive as `"2026-07-25 09:49"`. `JiraClient.to_bq_timestamp` cannot help:
    it only parses ISO-with-offset, so those values would silently become NULL.

    Output is canonical `YYYY-MM-DD HH:MM:SS`, which BigQuery accepts without
    relying on whether it tolerates a missing seconds component. Because the
    source is free text, an unparseable value becomes None rather than failing
    the load job for the entire table.

    DATETIME rather than TIMESTAMP is deliberate: the source carries no offset,
    so there is no zone to convert from and a TIMESTAMP would imply one.
    """
    if not isinstance(raw, str) or not raw.strip():
        return None

    value = raw.strip().replace("T", " ")

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    logger.warning("Unparseable datetime value %r; storing NULL", raw)
    return None


def parse_utc_timestamp(raw: Any) -> Optional[str]:
    """Parse a zone-less datetime string known to be UTC into a TIMESTAMP literal.

    For fields whose source carries no offset but whose data model specifies UTC,
    such as the IIM incident timing fields. The UTC zone is attached explicitly
    rather than relying on `.astimezone()`, which would interpret a naive
    datetime as the container's local time and make the same input produce
    different output depending on where the DAG ran.

    Returns None on unparseable input, like the other parsers, so a free-text
    field cannot fail the load for the whole table.
    """
    normalized = parse_datetime(raw)
    if normalized is None:
        return None

    return (
        datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=timezone.utc)
        .isoformat(timespec="seconds")
    )


def parse_date(raw: Any) -> Optional[str]:
    """Normalize a Jira date string to a BigQuery DATE literal, or None.

    Accepts a bare `YYYY-MM-DD` and also a full datetime, keeping the date part,
    so a field that changes type in Jira does not start returning NULLs.
    """
    if not isinstance(raw, str) or not raw.strip():
        return None

    value = raw.strip().replace("T", " ").split(" ")[0]

    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        logger.warning("Unparseable date value %r; storing NULL", raw)
        return None


def build_schema(extra_fields: Sequence[JiraField] = ()) -> list[bigquery.SchemaField]:
    """Return the base schema with any extra fields appended, in declared order."""
    base_names = {field.name for field in OUTPUT_SCHEMA}
    seen: set[str] = set()

    for field in extra_fields:
        if field.column in base_names:
            raise ValueError(
                f"extra field {field.column!r} collides with a base schema column; "
                "choose a different column name"
            )
        if field.column in seen:
            raise ValueError(f"duplicate extra field column {field.column!r}")
        seen.add(field.column)

    return list(OUTPUT_SCHEMA) + [
        bigquery.SchemaField(field.column, field.bq_type, mode=field.mode)
        for field in extra_fields
    ]


class BigQueryAPI:
    """BigQuery operations used by the Jira integration."""

    def __init__(self) -> None:
        """Initialize a logger for BigQuery load operations."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_issue_data(
        self,
        destination_table: str,
        issues: list[dict],
        schema: Optional[list[bigquery.SchemaField]] = None,
    ):
        """Load Jira issue records into the destination BigQuery table.

        `schema` defaults to the base schema, so an existing caller that passes
        only a destination and rows keeps its current behaviour.
        """
        job_config = bigquery.LoadJobConfig(
            schema=schema if schema is not None else OUTPUT_SCHEMA,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        client = bigquery.Client()
        job = client.load_table_from_json(
            issues, destination_table, job_config=job_config
        )
        job.result()
        self.logger.info("Loaded %s records to %s", len(issues), destination_table)


class JiraAPI:
    """Client for reading issue data from the Jira search API."""

    SEARCH_ENDPOINT = "/rest/api/3/search/jql"

    def __init__(
        self,
        base_jira_url: str,
        jql: str,
        extra_fields: Sequence[JiraField] = (),
    ) -> None:
        """Create an authenticated Jira API client from environment credentials."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = JiraClient(base_jira_url)
        self.jql = jql
        self.extra_fields = tuple(extra_fields)
        # Validate the column set now, so a bad declaration fails before any
        # network or BigQuery call rather than partway through a run.
        build_schema(self.extra_fields)

    def _requested_fields(self) -> list[str]:
        """Return base fields plus each extra field's Jira id, de-duplicated."""
        requested = list(REQUEST_FIELDS)
        for field in self.extra_fields:
            if field.jira_field not in requested:
                requested.append(field.jira_field)
        return requested

    def _extra_values(self, fields: dict) -> dict:
        """Project the declared extra fields off one issue's `fields` object."""
        values = {}

        for field in self.extra_fields:
            raw = fields.get(field.jira_field)

            if field.bq_type == "TIMESTAMP" and field.utc_naive:
                value: Any = parse_utc_timestamp(extract_value(raw))
            elif field.bq_type == "TIMESTAMP":
                # extract_value first: to_bq_timestamp hands its argument to
                # strptime, which raises TypeError on a dict or list, and only
                # ValueError is caught -- so an object-valued field would escape
                # and kill the whole run instead of nulling one cell.
                value = self.client.to_bq_timestamp(extract_value(raw))
            elif field.bq_type == "DATETIME":
                value = parse_datetime(extract_value(raw))
            elif field.bq_type == "DATE":
                value = parse_date(extract_value(raw))
            else:
                value = extract_value(raw)

            # A field Jira omitted must still appear on the row: NDJSON loads
            # infer nothing from an absent key, and a REPEATED column cannot
            # take null. A single-valued multi-select arrives unwrapped, so wrap
            # it rather than discarding it.
            if field.mode == "REPEATED":
                if value is None:
                    value = []
                elif not isinstance(value, list):
                    value = [value]
            values[field.column] = value

        return values

    def get_issues(self, max_results: int = 100) -> list[dict]:
        """Fetch issues from Jira and map fields to the BigQuery output schema."""
        issues = []

        for issue in self.client.paginate_token(
            self.SEARCH_ENDPOINT,
            {
                "maxResults": max_results,
                "jql": self.jql,
                "fields": ",".join(self._requested_fields()),
            },
            "issues",
        ):
            fields = issue.get("fields", {})
            issues.append(
                {
                    "issue_key": issue.get("key"),
                    "project_key": (fields.get("project") or {}).get("key"),
                    "issue_type": (fields.get("issuetype") or {}).get("name"),
                    "summary": fields.get("summary"),
                    "status": (fields.get("status") or {}).get("name"),
                    "created": self.client.to_bq_timestamp(fields.get("created")),
                    "resolved": self.client.to_bq_timestamp(
                        fields.get("resolutiondate")
                    ),
                    "updated": self.client.to_bq_timestamp(fields.get("updated")),
                    **self._extra_values(fields),
                }
            )

        return issues


class JiraIssueBigQueryIntegration:
    """Orchestrate fetching Jira issues and loading them to BigQuery."""

    def __init__(self) -> None:
        """Initialize a logger for integration-level progress messages."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, args, extra_fields: Sequence[JiraField] = ()):
        """Run the Jira-to-BigQuery integration with parsed CLI arguments.

        `extra_fields` is an explicit parameter rather than an attribute read off
        `args`, so a table that wants nothing extra calls `run(args)` exactly as
        before and there is no namespace contract for a copied query.py to omit.
        """
        self.logger.info("Starting Jira Issue BigQuery Integration ...")

        jira = JiraAPI(
            base_jira_url=args.base_jira_url, jql=args.jql, extra_fields=extra_fields
        )
        bq_api = BigQueryAPI()

        issues = jira.get_issues()
        self.logger.info(
            "Fetched %s Jira issues with %s extra field(s)",
            len(issues),
            len(tuple(extra_fields)),
        )

        bq_api.load_issue_data(
            args.destination, issues, schema=build_schema(extra_fields)
        )

        self.logger.info("End of Jira Issue BigQuery Integration")
