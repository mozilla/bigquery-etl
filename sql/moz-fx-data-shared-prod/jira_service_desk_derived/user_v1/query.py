from argparse import ArgumentParser
import requests
from requests.auth import HTTPBasicAuth
import json
import os
import sys

from google.cloud import bigquery
import google.auth
import logging


class BigQueryAPI:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_jira_user_data(self, destination_table: str, users: list[dict], truncate=True):
        """Load downloaded data to BQ table.
        https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json#python
        """
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
        client = bigquery.Client(credentials=credentials, project=project)
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if truncate else bigquery.WriteDisposition.WRITE_APPEND
        
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("account_id", "STRING"),
                bigquery.SchemaField("account_status", "STRING"),                
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("timeZone", "STRING"),
                bigquery.SchemaField("emailAddress", "STRING"),
                bigquery.SchemaField("accountType", "STRING"),
            ],
            autodetect=False,
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        job = client.load_table_from_json(
            users, destination_table, job_config=job_config
        )
        job.result()


class JiraAPI:
    def __init__(self, args) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

        self.secrets_dict = {
          "jira_username": "$JIRA_USERNAME",
          "jira_token": "$JIRA_TOKEN",
        }
        self.secrets_dict = {key:os.path.expandvars(self.secrets_dict[key]) for (key,value) in self.secrets_dict.items() }
        self.base_jira_url = args.base_jira_url
        self.auth = HTTPBasicAuth(self.secrets_dict.get('jira_username'), self.secrets_dict.get('jira_token'))

    def get_users_paged(self, max_results=500):
        startAt = 0
        headers = {"Accept": "application/json"}

        while True:           
            url = (
                self.base_jira_url
                + f"/rest/api/3/users/search?query=+&maxResults={max_results}&startAt={startAt}"
            )
            try:
                response = requests.request("GET", url, headers=headers, auth=self.auth)
            except Exception as e:
                self.logger.error(str(e))
                self.logger.critical("Failed while getting Jira users")
                sys.exit(1)
            is_success = 299 >= response.status_code >= 200
            if not is_success:
                self.logger.error(f"ERROR: response.status_code = {response.status_code}")
                self.logger.error(f"ERROR: response.text = {response.text}")
                self.logger.error(f"ERROR: response.reason = {response.reason}")
                self.logger.critical("Failed while getting Jira users")
                sys.exit(1)
                
            users = response.json()
            yield [
                {
                    "account_id": user.get("accountId", ""),
                    "account_status": (
                        "active" if user.get("active", "") else "inactive"
                    ),                    
                    "name": user.get("displayName", ""),
                    "timeZone": user.get("locale", ""),
                    "emailAddress": user.get("emailAddress", ""),
                    "accountType": user.get("accountType", ""),
                }
                for user in users
            ] 
            
            if len(users)<max_results:
                break
            
            startAt +=max_results    
             
             
class JiraBigQueryIntegration:

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, args):
        
        self.logger.info("Starting Jira BigQuery Integration ...")
        jira = JiraAPI(args)
        bigquery = BigQueryAPI()
        
        truncate = True          
        for users in jira.get_users_paged():                       
            bigquery.load_jira_user_data(args.destination, users,truncate)
            self.logger.info(f"Added {len(users)} users to user table")
            truncate = False    
        
        self.logger.info("End of Jira BigQuery Integration")
        
        
def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default="moz-fx-data-shared-prod.jira_service_desk_derived.user_v1",
        required=False,
    )
    parser.add_argument(
        "--base-url",
        dest="base_jira_url",
        default="https://mozilla-hub-sandbox-721.atlassian.net",
        required=False
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging._checkLevel("INFO"),
        encoding="utf-8",
    )

    integration = JiraBigQueryIntegration()
    integration.run(args)


main()
