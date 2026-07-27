"""Fetch Jira ticket change events and load them into BigQuery."""

import json
import logging
import tempfile
from argparse import ArgumentParser
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Iterator, Optional

from google.cloud import bigquery

from .client import JiraClient

EVENT_SCHEMA = [
    bigquery.SchemaField("issue_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("project_key", "STRING"),
    bigquery.SchemaField("issue_type", "STRING"),
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("actor_id", "STRING"),
    bigquery.SchemaField("actor_name", "STRING"),
    bigquery.SchemaField("field", "STRING"),
    bigquery.SchemaField("field_type", "STRING"),
    bigquery.SchemaField("from_value", "STRING"),
    bigquery.SchemaField("to_value", "STRING"),
    bigquery.SchemaField("from_id", "STRING"),
    bigquery.SchemaField("to_id", "STRING"),
    bigquery.SchemaField("comment_is_public", "BOOL"),
]

# Jira writes a synthetic "Last Comment" changelog entry on every comment, which
# would both double-count `commented` events and smuggle full comment text into
# from_value/to_value. "Rank" is backlog-reordering noise.
EXCLUDED_CHANGELOG_FIELDS = frozenset({"Last Comment", "Rank"})

# Fields whose changelog values are free-form ticket text: keep the event, drop
# the content. Jira puts the entire old and new description in
# fromString/toString, so a single edit would otherwise write two complete
# ticket bodies into from_value/to_value - the same content class the "no comment
# bodies" decision exists to avoid, and it makes row size unbounded. `summary` is
# deliberately absent: it is a short bounded title and jira_tickets_derived
# already stores it as plain text.
REDACTED_CHANGELOG_FIELDS = frozenset({"Comment", "description"})

SEARCH_FIELDS = ["project", "issuetype", "created", "reporter"]

CHANGELOG_ITEMS_KEY = "values"
PAGE_SIZE = 100

logger = logging.getLogger(__name__)


def _event(
    issue: dict,
    *,
    event_id: str,
    event_type: str,
    event_ts: str,
    actor: Optional[dict] = None,
    field: Optional[str] = None,
    field_type: Optional[str] = None,
    from_value: Optional[str] = None,
    to_value: Optional[str] = None,
    from_id: Optional[str] = None,
    to_id: Optional[str] = None,
    comment_is_public: Optional[bool] = None,
) -> dict:
    """Build a row carrying every EVENT_SCHEMA column, defaulting to None."""
    actor = actor or {}
    return {
        "issue_key": issue["issue_key"],
        "project_key": issue["project_key"],
        "issue_type": issue["issue_type"],
        "event_id": event_id,
        "event_type": event_type,
        "event_ts": event_ts,
        "actor_id": actor.get("accountId"),
        "actor_name": actor.get("displayName"),
        "field": field,
        "field_type": field_type,
        "from_value": from_value,
        "to_value": to_value,
        "from_id": from_id,
        "to_id": to_id,
        "comment_is_public": comment_is_public,
    }


def created_event(issue: dict) -> Optional[dict]:
    """Build the synthetic `created` event from the issue's own fields."""
    event_ts = issue.get("created")
    if not event_ts:
        logger.warning(
            "Skipping created event for %s: no created timestamp", issue["issue_key"]
        )
        return None

    return _event(
        issue,
        event_id="created",
        event_type="created",
        event_ts=event_ts,
        actor=issue.get("reporter"),
    )


def changelog_events(issue: dict, history: dict, to_ts: Callable) -> Iterator[dict]:
    """Yield one event per changelog item.

    Items naming an EXCLUDED_CHANGELOG_FIELDS field are dropped entirely. Items
    naming a REDACTED_CHANGELOG_FIELDS field are kept, but their from_value and
    to_value are blanked so free-form ticket text never reaches the table.
    """
    event_ts = to_ts(history.get("created"))
    if not event_ts:
        logger.warning(
            "Skipping changelog %s on %s: unparseable timestamp %r",
            history.get("id"),
            issue["issue_key"],
            history.get("created"),
        )
        return

    author = history.get("author") or {}

    for item in history.get("items") or []:
        field = item.get("field")
        if field in EXCLUDED_CHANGELOG_FIELDS:
            continue

        redacted = field in REDACTED_CHANGELOG_FIELDS

        yield _event(
            issue,
            event_id=str(history.get("id")),
            event_type="status_change" if field == "status" else "field_change",
            event_ts=event_ts,
            actor=author,
            field=field,
            field_type=item.get("fieldtype"),
            from_value=None if redacted else item.get("fromString"),
            to_value=None if redacted else item.get("toString"),
            from_id=item.get("from"),
            to_id=item.get("to"),
        )


def _comment_is_public(comment: dict) -> bool:
    """Report whether a comment is visible to all project users."""
    if "jsdPublic" in comment:
        return bool(comment["jsdPublic"])
    # Truthiness rather than key presence: Jira may send `"visibility": null`
    # for an unrestricted comment, and a key-presence test would then mark every
    # comment non-public.
    return not comment.get("visibility")


def comment_event(issue: dict, comment: dict, to_ts: Callable) -> Optional[dict]:
    """Build a `commented` event from a comment payload, without its body."""
    event_ts = to_ts(comment.get("created"))
    if not event_ts:
        logger.warning(
            "Skipping comment %s on %s: unparseable timestamp %r",
            comment.get("id"),
            issue["issue_key"],
            comment.get("created"),
        )
        return None

    return _event(
        issue,
        event_id=str(comment.get("id")),
        event_type="commented",
        event_ts=event_ts,
        actor=comment.get("author"),
        comment_is_public=_comment_is_public(comment),
    )


class JiraEventsAPI:
    """Read issue change events from the Jira REST API."""

    SEARCH_ENDPOINT = "/rest/api/3/search/jql"
    CHANGELOG_ENDPOINT = "/rest/api/3/issue/{issue_key}/changelog"
    COMMENT_ENDPOINT = "/rest/api/3/issue/{issue_key}/comment"

    def __init__(self, client, jql: str) -> None:
        """Wrap a JiraClient with the JQL selecting issues in scope."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = client
        self.jql = jql

    def get_issues(self) -> list[dict]:
        """Fetch every issue matching the JQL, keeping only fields events need."""
        issues = []

        for raw in self.client.paginate_token(
            self.SEARCH_ENDPOINT,
            {
                "jql": self.jql,
                "maxResults": PAGE_SIZE,
                "fields": ",".join(SEARCH_FIELDS),
            },
            "issues",
        ):
            fields = raw.get("fields") or {}
            issues.append(
                {
                    "issue_key": raw.get("key"),
                    "project_key": (fields.get("project") or {}).get("key"),
                    "issue_type": (fields.get("issuetype") or {}).get("name"),
                    "created": self.client.to_bq_timestamp(fields.get("created")),
                    "reporter": fields.get("reporter") or {},
                }
            )

        self.logger.info("Found %s issues in scope", len(issues))
        return issues

    def _fetch_changelog(self, issue_key: str) -> Iterator[dict]:
        """Yield changelog histories for an issue, failing loudly on an unexpected page shape."""
        path = self.CHANGELOG_ENDPOINT.format(issue_key=issue_key)
        params = {"maxResults": PAGE_SIZE}
        page = self.client.get(path, {**params, "startAt": 0})

        if CHANGELOG_ITEMS_KEY not in page and page.get("total"):
            raise RuntimeError(
                f"Changelog response for {issue_key} has no {CHANGELOG_ITEMS_KEY!r} key but reports "
                f"total={page.get('total')}; keys were {sorted(page)}. The Jira changelog pagination "
                "shape has changed - update CHANGELOG_ITEMS_KEY."
            )

        # The validated page is handed to the paginator rather than re-fetched:
        # asking for startAt=0 twice per issue would nearly double a seed's call
        # count, and 429s are the binding constraint on a seed.
        yield from self.client.paginate_start_at(
            path, params, CHANGELOG_ITEMS_KEY, first_page=page
        )

    def iter_events(self, issues: list[dict]) -> Iterator[dict]:
        """Yield every event for the given issues, one issue at a time."""
        for index, issue in enumerate(issues, start=1):
            issue_key = issue["issue_key"]

            if index % 100 == 0:
                self.logger.info("Fetched events for %s/%s issues", index, len(issues))

            created = created_event(issue)
            if created:
                yield created

            for history in self._fetch_changelog(issue_key):
                yield from changelog_events(issue, history, self.client.to_bq_timestamp)

            for comment in self.client.paginate_start_at(
                self.COMMENT_ENDPOINT.format(issue_key=issue_key),
                {"maxResults": PAGE_SIZE},
                "comments",
            ):
                event = comment_event(issue, comment, self.client.to_bq_timestamp)
                if event:
                    yield event


def events_on_date(events: Iterable[dict], target_date: date) -> Iterator[dict]:
    """Yield only events whose UTC event date matches target_date."""
    prefix = target_date.isoformat()

    for event in events:
        if (event.get("event_ts") or "").startswith(prefix):
            yield event


class EventsBigQueryAPI:
    """Load Jira event rows into a partitioned BigQuery table."""

    def __init__(self) -> None:
        """Initialize a logger for load operations."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_events(
        self,
        destination: str,
        events: Iterable[dict],
        date_partition: Optional[str] = None,
        write_append: bool = False,
    ) -> int:
        """Stream events through a temporary NDJSON file into BigQuery.

        Events are written to disk as they are produced so that memory stays
        constant regardless of how many there are. Returns the row count.
        """
        job_config = bigquery.LoadJobConfig(
            schema=EVENT_SCHEMA,
            autodetect=False,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_APPEND
                if write_append
                else bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="event_ts"
            ),
            # A WRITE_TRUNCATE against the base table (every --seed) replaces the
            # table definition, so the clustering declared in metadata.yaml has to
            # be restated here or the seeded table comes back unclustered.
            clustering_fields=["project_key", "issue_key"],
        )

        table = f"{destination}${date_partition}" if date_partition else destination
        client = bigquery.Client()
        count = 0

        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".ndjson") as tmp:
            for event in events:
                tmp.write(json.dumps(event).encode("utf-8"))
                tmp.write(b"\n")
                count += 1

            tmp.flush()
            tmp.seek(0)

            job = client.load_table_from_file(tmp, table, job_config=job_config)
            job.result()

        self.logger.info("Loaded %s events to %s", count, table)
        return count


def daily_jql(jql: str, target_date: date) -> str:
    """Build JQL selecting every issue that could have an event on target_date.

    There is deliberately **no upper bound**. Every event this table records bumps
    the issue's `updated`: creation sets it, a changelog write is by definition a
    modification, and commenting updates it too. So any issue with an event on D
    necessarily has `updated >= D`, which means `updated >= D-1` on its own already
    selects a superset of the issues with events on D, and `events_on_date` filters
    precisely from there.

    An `updated < D+2` clause would buy nothing on the scheduled run - at 04:00 on
    D+1 no issue can have `updated >= D+2` - while being the one thing that makes
    re-running an old date lossy, because it drops issues touched again since.
    Without it, re-running D fetches more issues the further back D is, but yields a
    complete partition, so clearing the task in Airflow is an ordinary repair.

    The lower bound is widened by a day because JQL date literals are evaluated in
    the token owner's Jira profile timezone rather than UTC; a day of slack covers
    every offset from UTC-12 to UTC+14.
    """
    lower = target_date - timedelta(days=1)
    return f'({jql}) AND updated >= "{lower.isoformat()}"'


def build_parser(destination: str, base_jira_url: str, jql: str) -> ArgumentParser:
    """Build the CLI for a Jira events table, with this table's defaults baked in.

    Each table's `query.py` is a handful of lines calling this, rather than its own
    copy of the flag definitions. That matters for two reasons: the guard semantics
    stay identical across tables instead of drifting as files are copied, and
    `default_jql` is set here, where it is consumed, instead of being assigned onto
    the namespace by hand after `parse_args()`. A copied `query.py` that forgot that
    assignment used to make every `--seed` run die with `AttributeError`, and
    `--seed` is the one path neither CI nor a scheduled run exercises.
    """
    parser = ArgumentParser(description=f"Load Jira change events into {destination}.")
    parser.add_argument("--destination", dest="destination", default=destination)
    parser.add_argument("--base-url", dest="base_jira_url", default=base_jira_url)
    parser.add_argument(
        "--jql",
        dest="jql",
        default=jql,
        help=(
            "Override the table's scope. Refused by every mode that writes: a "
            "narrower JQL would WRITE_TRUNCATE the target with only what it matched."
        ),
    )
    parser.add_argument(
        "--date",
        dest="date",
        default=None,
        help=(
            "Rebuild this UTC date's partition (YYYY-MM-DD). Required unless --seed. "
            "Safe to re-run for any past date, including by clearing the Airflow task."
        ),
    )
    parser.add_argument(
        "--seed",
        dest="seed",
        action="store_true",
        help=(
            "Rebuild the whole table from full issue history. Manual use only; needed "
            "for the initial load."
        ),
    )
    parser.add_argument(
        "--since",
        dest="since",
        default=None,
        help="With --seed, only issues created on or after this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--until",
        dest="until",
        default=None,
        help="With --seed, only issues created before this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--write-append",
        dest="write_append",
        action="store_true",
        help=(
            "Append instead of truncating. Use for the second and later chunks of a "
            "chunked seed; re-running a chunk duplicates rows, so restart from the "
            "failed chunk onward."
        ),
    )
    parser.add_argument(
        "--write-truncate",
        dest="write_truncate",
        action="store_true",
        help=(
            "Truncate the whole table before loading. Required to make the intent "
            "explicit for the first chunk of a chunked seed; a bounded seed "
            "(--since/--until) refuses to run without exactly one of "
            "--write-truncate or --write-append."
        ),
    )
    # Not a flag: the guard needs the table's configured scope to compare against,
    # so it must survive an explicit --jql override.
    parser.set_defaults(default_jql=jql)
    return parser


def seed_jql(jql: str, since: Optional[str], until: Optional[str]) -> str:
    """Build JQL for a seed run, optionally bounded by issue creation date."""
    clauses = [f"({jql})"]
    if since:
        clauses.append(f'created >= "{since}"')
    if until:
        clauses.append(f'created < "{until}"')
    return " AND ".join(clauses)


class JiraEventsBigQueryIntegration:
    """Orchestrate fetching Jira events and loading them to BigQuery."""

    def __init__(self) -> None:
        """Initialize a logger for integration-level progress messages."""
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _validate(args) -> None:
        """Reject argument combinations that would silently lose or duplicate data.

        `args` must carry every one of these attributes; there is no default for any
        of them, and a missing one is an `AttributeError` at run time:

        - `destination` (str) — fully qualified `project.dataset.table`.
        - `base_jira_url` (str) — Jira instance root.
        - `jql` (str) — the scope of the table.
        - `default_jql` (str) — `query.py`'s built-in `--jql` default, used to detect
          an explicitly narrowed seed. **This is not an argparse flag.** `query.py`
          assigns it onto the namespace after `parse_args()`
          (`args.default_jql = DEFAULT_JQL`). A copied `query.py` that omits that
          line makes every `--seed` run die with `AttributeError`.
        - `date` (str | None) — `YYYY-MM-DD`, the partition to rebuild.
        - `seed` (bool), `since` (str | None), `until` (str | None).
        - `write_append` (bool), `write_truncate` (bool).

        `date`, `since`, and `until` are strings, not `date` objects; `date` is parsed
        here so a malformed value fails before any network or BigQuery call.
        """
        if args.jql != args.default_jql:
            raise ValueError(
                "this run refuses a custom --jql: every run WRITE_TRUNCATEs its whole "
                "target (the base table for --seed, one partition for --date) with "
                "only the issues the JQL matched, so a narrower scope silently drops "
                "everything it did not fetch. Bound a seed with --since/--until instead"
            )

        if args.seed:
            if args.date:
                raise ValueError("--seed and --date are mutually exclusive")
                raise ValueError(
                    "--seed refuses a custom --jql because its WRITE_TRUNCATE would "
                    "discard history the seed did not fetch; narrow a seed with "
                    "--since/--until instead"
                )
            if args.since or args.until:
                if args.write_append == args.write_truncate:
                    raise ValueError(
                        "a bounded seed (--since/--until) must pass exactly one of "
                        "--write-append or --write-truncate: it would otherwise default to "
                        "WRITE_TRUNCATE on the whole table while loading only the bounded "
                        "subset, discarding history it never fetched"
                    )
            elif args.write_append:
                raise ValueError(
                    "an unbounded --seed cannot use --write-append: it fetches the "
                    "table's whole history, so appending it on top of the rows already "
                    "there duplicates every event, and the duplicates are "
                    "indistinguishable (same issue_key, event_id and field). Drop "
                    "--write-append to rebuild with WRITE_TRUNCATE, or bound this chunk "
                    "with --since/--until"
                )
            return

        if not args.date:
            raise ValueError("--date is required unless --seed is given")

        seed_only = [
            name
            for name, value in (
                ("--since", args.since),
                ("--until", args.until),
                ("--write-append", args.write_append),
                ("--write-truncate", args.write_truncate),
            )
            if value
        ]
        if seed_only:
            raise ValueError(
                f"cannot combine --date with {', '.join(seed_only)}; those flags "
                "only apply to --seed and would be silently ignored, because a --date "
                "run always WRITE_TRUNCATEs exactly the one partition it just fetched"
            )

        # Parse here so a malformed --date fails before any network or BigQuery
        # call. No staleness check is needed: `daily_jql` has no upper bound, so
        # re-running an arbitrarily old date still rebuilds its partition completely.
        datetime.strptime(args.date, "%Y-%m-%d")

    def run(self, args) -> None:
        """Run the Jira events integration with parsed CLI arguments.

        See `_validate` for the full set of attributes `args` must carry.
        """
        self._validate(args)
        self.logger.info("Starting Jira Events BigQuery Integration ...")

        client = JiraClient(args.base_jira_url)
        bq_api = EventsBigQueryAPI()

        if args.seed:
            jql = seed_jql(args.jql, args.since, args.until)
            self.logger.info("Seeding with JQL: %s", jql)

            api = JiraEventsAPI(client, jql)
            events = api.iter_events(api.get_issues())

            bq_api.load_events(
                args.destination,
                events=events,
                date_partition=None,
                write_append=args.write_append,
            )
        else:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            jql = daily_jql(args.jql, target_date)
            self.logger.info("Loading %s with JQL: %s", target_date, jql)

            api = JiraEventsAPI(client, jql)
            events = events_on_date(api.iter_events(api.get_issues()), target_date)

            bq_api.load_events(
                args.destination,
                events=events,
                date_partition=target_date.strftime("%Y%m%d"),
                write_append=False,
            )

        self.logger.info("End of Jira Events BigQuery Integration")
