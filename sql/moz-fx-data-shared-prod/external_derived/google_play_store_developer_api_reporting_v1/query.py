import json
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import requests

# Path to your service account JSON key file
SERVICE_ACCOUNT_FILE = "/Users/kwindau/Documents/2025/202502/boxwood-axon-825-a8f9a0239d65.json" #TEMP - will replace with variable later on

# Define the required scope for Play Developer Reporting API
SCOPES = ["https://www.googleapis.com/auth/playdeveloperreporting"]

# Authenticate using the service account JSON
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

# Get an access token
credentials.refresh(Request())
access_token = credentials.token

# Define the API URL
url = "https://playdeveloperreporting.googleapis.com/v1beta1/apps/org.mozilla.firefox/crashRateMetricSet"

# Set up the headers with the access token
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# Make the GET request
response = requests.get(url, headers=headers, timeout=10)

# Print the response
print(response.status_code)
print(response.json())
