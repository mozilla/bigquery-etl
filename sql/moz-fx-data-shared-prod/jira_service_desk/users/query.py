from argparse import ArgumentParser
import requests
from requests.auth import HTTPBasicAuth
import json
import os 

from google.cloud import bigquery
import google.auth
import logging

 
class BigQueryAPI:
  def __init__(self) -> None:
    self.logger = logging.getLogger(self.__class__.__name__)
  
  def load_jira_user_data(self, destination_table: str, users: list[dict]):
    """Load downloaded data to BQ table."""
    credentials, project = google.auth.default( 
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    client = bigquery.Client(credentials=credentials, project=project)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("account_status", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("name", "STRING"),
        ],
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    
    job = client.load_table_from_json(
        users, destination_table, job_config=job_config
    )
    job.result()

class JiraAPI():  
  def __init__(self,args) -> None:
    self.logger = logging.getLogger(self.__class__.__name__)
    
    # self.secrets_dict = {
    #   "jira_username": "$fivetran_jira_integration_username",
    #   "jira_password": "$fivetran_jira_integration_token",
    # }
    # self.secrets_dict = {key:os.path.expandvars(self.secrets_dict[key]) for (key,value) in self.secrets_dict.items() } 
    self.base_url = "https://mozilla-hub-sandbox-721.atlassian.net"
    self.auth = HTTPBasicAuth(args.jira_username, args.jira_token)
    
  def get_users_paged(self, max_results=300):    
    startAt = 0 
    headers = {
        "Accept": "application/json"
      }
        
    while True:
      url = self.base_url + f"/rest/api/3/users/search?query=+&maxResults={max_results}&startAt={startAt}"
      response = requests.request(
                  "GET",
                  url,
                  headers=headers,
                  auth=self.auth
                )     
      yield [{"account_id":user.get('accountId',''),
                    "account_status":"active" if user.get('active','') else "inactive",
                    "email":user.get('emailAddress',''),
                    "name":user.get('displayName',''),
                    } for user in json.loads(response.text)]
      
      # TODO
      break
      # startAt +=max_results
  
    
class JiraBigQueryIntegration():
  
  def __init__(self) -> None:
     self.logger = logging.getLogger(self.__class__.__name__)
     
  def run(self, args):
    jira = JiraAPI(args)
    bigquery = BigQueryAPI()
    destination_table = "moz-fx-data-shared-prod.jira_service_desk.user"
    
    while True:
      max_results = 2 # initial tests
       
      for users in jira.get_users_paged(max_results):
        bigquery.load_jira_user_data(destination_table, users)
        if len(users)<max_results:
          break
       
      
def main():    
  parser = ArgumentParser()
  parser.add_argument("--fivetran-jira-integration-jira-username", dest="jira_username", required=True)
  parser.add_argument("--fivetran-jira-integration-jira-token", dest="jira_token", required=True)
  
  args = parser.parse_args()
  
  log_level = logging._checkLevel('INFO')
  logging.basicConfig(
            format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
            level=log_level,
            encoding="utf-8",
        )  
  
  integration = JiraBigQueryIntegration()
  integration.run(args)
  
   
main()