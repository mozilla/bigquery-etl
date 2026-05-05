import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from bigquery_etl.jira import JiraIssueBigQueryIntegration

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default="moz-fx-data-shared-prod.jira_tickets_derived.ffxp_epic_issues_v1",
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
        default="project = FFXP AND issuetype = Epic ORDER BY created DESC",
        required=False,
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8",
    )

    integration = JiraIssueBigQueryIntegration()
    integration.run(args)
