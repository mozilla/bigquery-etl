import requests
from requests.auth import HTTPBasicAuth

base_URL = "https://mozilla-np.xmatters.com/api/xm/1"

person_name = "342d509e-6ae3-4c0a-bd59-b07cdc7c6eb3"
endpoint_URL = "/people/" + person_name + "/supervisors"

url = base_URL + endpoint_URL

print("Sending request to url: " + url)
auth = HTTPBasicAuth("serviceuser", "welcome1")

response = requests.get(url, auth=auth)

responseCode = response.status_code
if responseCode == 200:
    rjson = response.json()
    for d in rjson.get("data"):
        print(
            'User "'
            + person_name
            + '" has supervisor "'
            + d["targetName"]
            + '" with first name "'
            + d["firstName"]
            + '" and last name "'
            + d["lastName"]
            + '"'
        )
