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
        # Union, not just `project = SREIN`: intake tickets get triaged and *moved*
        # to the owning team's project, which changes their key and drops them out
        # of a project-scoped JQL, taking their whole history with them. The SREIN
        # label survives a move, so it recovers them. But the label is not on every
        # in-project ticket either — older ones predate the automation that applies
        # it, and some carry only `form`/`form-1344` — so a label-only scope would
        # drop those. Both halves are needed.
        jql="project = SREIN OR labels = SREIN",
    ).parse_args()

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8",
    )

    JiraEventsBigQueryIntegration().run(args)
