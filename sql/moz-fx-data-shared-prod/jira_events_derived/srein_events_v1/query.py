"""Load Jira change events for the SREIN project into BigQuery."""

import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from bigquery_etl.jira import JiraEventsBigQueryIntegration  # noqa: E402

DEFAULT_JQL = "project = SREIN"

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default="moz-fx-data-shared-prod.jira_events_derived.srein_events_v1",
        required=False,
    )
    parser.add_argument(
        "--base-url",
        dest="base_jira_url",
        default="https://mozilla-hub.atlassian.net",
        required=False,
    )
    parser.add_argument(
        "--jql",
        dest="jql",
        default=DEFAULT_JQL,
        required=False,
    )
    parser.add_argument(
        "--date",
        dest="date",
        default=None,
        required=False,
        help="Load events for this UTC date (YYYY-MM-DD). Required unless --seed.",
    )
    parser.add_argument(
        "--seed",
        dest="seed",
        action="store_true",
        help=(
            "Rebuild the whole table from full issue history. Manual use only: a "
            "per-day re-run cannot repair old partitions because JQL `updated` only "
            "reflects an issue's most recent update."
        ),
    )
    parser.add_argument(
        "--since",
        dest="since",
        default=None,
        required=False,
        help="With --seed, only issues created on or after this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--until",
        dest="until",
        default=None,
        required=False,
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

    args = parser.parse_args()
    args.default_jql = DEFAULT_JQL

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8",
    )

    integration = JiraEventsBigQueryIntegration()
    integration.run(args)
