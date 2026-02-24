import logging
from argparse import ArgumentParser

from bigquery_etl.jira import JiraIssueBigQueryIntegration


def main(default_destination: str, default_jql: str):
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default=default_destination,
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
        default=default_jql,
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
