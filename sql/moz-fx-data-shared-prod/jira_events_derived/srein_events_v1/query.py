"""Load Jira change events for the SREIN project into BigQuery."""

import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from bigquery_etl.jira import JiraEventsBigQueryIntegration, build_parser  # noqa: E402

if __name__ == "__main__":
    args = build_parser(
        destination="moz-fx-data-shared-prod.jira_events_derived.srein_events_v1",
        base_jira_url="https://mozilla-hub.atlassian.net",
        jql="project = SREIN",
    ).parse_args()

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8",
    )

    JiraEventsBigQueryIntegration().run(args)
