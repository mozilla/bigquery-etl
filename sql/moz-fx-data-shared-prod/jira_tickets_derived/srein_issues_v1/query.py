"""Load Jira issue snapshots for the SREIN support universe into BigQuery."""

import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from bigquery_etl.jira import JiraField, JiraIssueBigQueryIntegration  # noqa: E402

# Union, not just `project = SREIN`: intake tickets get triaged and moved to the
# owning team's project, which changes their key and drops them out of a
# project-scoped JQL. The SREIN label survives a move, so it recovers them. The
# label alone is not enough either, because older tickets predate the automation
# that applies it. Note the field is `labels`, plural -- JQL has no `label`.
DEFAULT_JQL = "project = SREIN OR labels = SREIN"

# Appended after the base schema, in this order. Keep in step with schema.yaml.
EXTRA_FIELDS = [
    JiraField("priority", "priority"),
    JiraField("assignee", "assignee"),
    JiraField("reporter", "reporter"),
    JiraField("resolution", "resolution"),
    JiraField("labels", "labels", mode="REPEATED"),
]

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default="moz-fx-data-shared-prod.jira_tickets_derived.srein_issues_v1",
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

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8",
    )

    integration = JiraIssueBigQueryIntegration()
    integration.run(args, extra_fields=EXTRA_FIELDS)
