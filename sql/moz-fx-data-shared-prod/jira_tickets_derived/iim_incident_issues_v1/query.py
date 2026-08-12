"""Load Jira incident snapshots for the IIM project into BigQuery."""

import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from bigquery_etl.jira import JiraField, JiraIssueBigQueryIntegration  # noqa: E402

# Field ids captured from IIM-308 via /rest/api/3/issue/{key}?expand=names, which
# is the only reliable mapping: the search API will not accept custom-field
# display names, and several names here are ambiguous. "Severity" in particular
# collides with "Severity Level" (customfield_11852) and "Requested Severity
# Level" (customfield_10716); customfield_10319 is the one shown on the incident
# form, and its stored name has a trailing space.
#
# The timing fields are free text in Jira holding a zone-less value like
# "2026-07-25 09:49". The incident data model specifies UTC (confirmed by the
# incident process owner in review), so they are TIMESTAMP with utc_naive: the
# zone is attached explicitly rather than inferred from the container's clock.
# That also lets them be diffed directly against created/updated/resolved.
# Plain TIMESTAMP would NULL every row, since the ISO-with-offset parser used for
# the base columns cannot read this format.
EXTRA_FIELDS = [
    JiraField("affected_entities", "customfield_18555"),
    JiraField("severity", "customfield_10319"),
    JiraField("detection_method", "customfield_12881"),
    JiraField("declare_date", "customfield_15087", bq_type="DATE"),
    JiraField("impact_start", "customfield_18693", bq_type="TIMESTAMP", utc_naive=True),
    JiraField(
        "time_declared", "customfield_18692", bq_type="TIMESTAMP", utc_naive=True
    ),
    JiraField(
        "time_detected", "customfield_18694", bq_type="TIMESTAMP", utc_naive=True
    ),
    JiraField("time_alerted", "customfield_18695", bq_type="TIMESTAMP", utc_naive=True),
    JiraField(
        "time_acknowledged", "customfield_18696", bq_type="TIMESTAMP", utc_naive=True
    ),
    JiraField(
        "time_responded", "customfield_18697", bq_type="TIMESTAMP", utc_naive=True
    ),
    JiraField(
        "time_mitigated", "customfield_18698", bq_type="TIMESTAMP", utc_naive=True
    ),
    JiraField(
        "time_resolved", "customfield_18699", bq_type="TIMESTAMP", utc_naive=True
    ),
]

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default="moz-fx-data-shared-prod.jira_tickets_derived.iim_incident_issues_v1",
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
        default="project = IIM AND issuetype = Incident ORDER BY created DESC",
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
