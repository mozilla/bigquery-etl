from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from jira_issues_etl import main


if __name__ == "__main__":
    main(
        default_destination="moz-fx-data-shared-prod.jira_tickets_derived.iim_incident_issues_v1",
        default_jql="project = IIM AND issuetype = Incident ORDER BY created DESC",
    )
