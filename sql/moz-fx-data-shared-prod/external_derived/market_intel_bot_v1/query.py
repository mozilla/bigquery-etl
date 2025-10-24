"""Summarize Chrome release updates from GCS files using OpenAI and write results back to GCS."""

# Load libraries
import json
import os

# import requests #Only needed once Slack added
import sys
from argparse import ArgumentParser
from datetime import datetime, date

from google.cloud import storage
from openai import OpenAI

# Set variables
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
BUCKET_NO_GS = "moz-fx-data-prod-external-data"

# Filepath for the final, consolidated report
FINAL_REPORT_FPATH = "MARKET_RESEARCH/FINAL_REPORTS/MarketIntelBotReport_"

# Filepaths to read the data loaded to GCS by the "release_scraping DAG"
INPUT_FPATH_2 = "MARKET_RESEARCH/SCRAPED_INFO/ChromeReleaseNotes/WebScraping_"
INPUT_FPATH_3 = "MARKET_RESEARCH/SCRAPED_INFO/ChromeAI/WebScraping_"
INPUT_FPATH_4 = "MARKET_RESEARCH/SCRAPED_INFO/ChromeDevTools/WebScraping_"

# Filepaths to save ChatGPT Summaries to
OUTPUT_FPATH_1 = "MARKET_RESEARCH/SUMMARY_INFO/BrowserDevelopment/WebScraping_"
OUTPUT_FPATH_2 = "MARKET_RESEARCH/SUMMARY_INFO/ChromeReleaseNotes/WebScraping_"
OUTPUT_FPATH_3 = "MARKET_RESEARCH/SUMMARY_INFO/ChromeAI/WebScraping_"
OUTPUT_FPATH_4 = "MARKET_RESEARCH/SUMMARY_INFO/ChromeDevTools/WebScraping_"
OUTPUT_FPATH_5 = "MARKET_RESEARCH/SUMMARY_INFO/DeviceMarketResearch/WebScraping_"
OUTPUT_FPATH_6 = "MARKET_RESEARCH/SUMMARY_INFO/OnlineAdvertising/WebScraping_"
OUTPUT_FPATH_7 = "MARKET_RESEARCH/SUMMARY_INFO/AI_News/WebScraping_"
OUTPUT_FPATH_8 = "MARKET_RESEARCH/SUMMARY_INFO/UpcomingHolidaysOrEventsImpactingBrowserUsage/WebScraping_"

# Pull in the API key from GSM
OPENAI_API_TOKEN = os.getenv("DATA_ENG_OPEN_AI_API_KEY")

# Pull in the Slack webhook URL from GSM
# SLACK_WEBHOOK = os.getenv("SLACK_MARKET_INTEL_BOT_WEBHOOK_URL")

# If the API token is not found, raise an error
if not OPENAI_API_TOKEN:
    raise ValueError("Environment variable DATA_ENG_OPEN_AI_API_KEY is not set!")

# If the slack webhook is not found, raise an error
# if not SLACK_WEBHOOK:
#     raise ValueError("Environment variable SLACK_MARKET_INTEL_BOT_WEBHOOK_URL is not set!")


def ensure_gcs_file_exists(gcs_path: str):
    """Check if a GCS file exists at the given path.

    Exit with an error if the file does not exist.

    Args:
        gcs_path (str): Full GCS path (e.g., 'gs://my-bucket/path/to/file.csv')
    """
    if not gcs_path.startswith("gs://"):
        print(f"Invalid GCS path: {gcs_path}")
        sys.exit(1)

    # Parse bucket and blob
    try:
        _, _, bucket_name, *path_parts = gcs_path.split("/", 3)
        blob_path = path_parts[0] if path_parts else ""
    except Exception:
        print(f"Unable to parse GCS path: {gcs_path}")
        sys.exit(1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if not blob.exists():
        print(f"File not found in GCS: {gcs_path}")
        sys.exit(1)
    else:
        print(f"File found: {gcs_path}")


def read_gcs_file(gcs_path: str) -> str:
    """Read the contents of a file in GCS and returns it as a string."""
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")

    # Parse bucket and blob
    _, _, bucket_name, *path_parts = gcs_path.split("/", 3)
    blob_path = path_parts[0] if path_parts else ""

    client = storage.Client(project="moz-fx-data-shared-prod")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if not blob.exists():
        raise FileNotFoundError(f"File not found: {gcs_path}")

    content = blob.download_as_text()  # or .download_as_bytes() for binary files
    return content


def main():
    """Pull scraped data from GCS, ask ChatGPT to summarize, save summary back to GCS."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    logical_dag_date_str = logical_dag_date.strftime("%Y%m%d")

    # Get the first day of the next month after the logical DAG date
    report_generation_year = logical_dag_date.year + (logical_dag_date.month // 12)
    report_generation_month = (logical_dag_date.month % 12) + 1
    report_date = date(report_generation_year, report_generation_month, 1).strftime(
        "%Y%m%d"
    )
    print("report_date: ")
    print(report_date)

    # Check both input files exist, if not, error out
    gcs_fpath2 = GCS_BUCKET + INPUT_FPATH_2 + logical_dag_date_str + ".txt"
    gcs_fpath3 = GCS_BUCKET + INPUT_FPATH_3 + logical_dag_date_str + ".txt"
    gcs_fpath4 = GCS_BUCKET + INPUT_FPATH_4 + logical_dag_date_str + ".txt"

    print("Checking to see if fpath 2 exists: ")
    print(gcs_fpath2)
    ensure_gcs_file_exists(gcs_fpath2)

    print("Checking to see if fpath 3 exists: ")
    print(gcs_fpath3)
    ensure_gcs_file_exists(gcs_fpath3)

    print("Checking to see if fpath 4 exists: ")
    print(gcs_fpath4)
    ensure_gcs_file_exists(gcs_fpath4)

    # Make the output fpaths for storing the summaries received from ChatGPT
    final_output_fpath1 = OUTPUT_FPATH_1 + logical_dag_date_str + ".md"
    final_output_fpath2 = OUTPUT_FPATH_2 + logical_dag_date_str + ".md"
    final_output_fpath3 = OUTPUT_FPATH_3 + logical_dag_date_str + ".md"
    final_output_fpath4 = OUTPUT_FPATH_4 + logical_dag_date_str + ".md"
    final_output_fpath5 = OUTPUT_FPATH_5 + logical_dag_date_str + ".md"
    final_output_fpath6 = OUTPUT_FPATH_6 + logical_dag_date_str + ".md"
    final_output_fpath7 = OUTPUT_FPATH_7 + logical_dag_date_str + ".md"
    final_output_fpath8 = OUTPUT_FPATH_8 + logical_dag_date_str + ".md"

    # Read in the scraped data from the 2 files
    file_contents2 = read_gcs_file(gcs_fpath2)
    file_contents3 = read_gcs_file(gcs_fpath3)
    file_contents4 = read_gcs_file(gcs_fpath4)

    # Initialize the final output as an empty string
    final_output_1 = ""
    final_output_2 = ""
    final_output_3 = ""
    final_output_4 = ""
    final_output_5 = ""
    final_output_6 = ""
    final_output_7 = ""
    final_output_8 = ""
    final_report = """# Market Intel Bot Report
Table of Contents:
* New Features in Popular Browsers
* New Features in Chrome
* New AI Features in Chrome
* New Features in Chrome Dev Tools
* Device & Browser Partnership News
* Online Advertising News
* AI News
* Upcoming Events Impacting Browser Usage
"""

    # Open an Open AI Client
    client = OpenAI(api_key=OPENAI_API_TOKEN)

    # Ask ChatGPT to search the web for recent updates on browser development
    prompt1 = (
        "Look for articles from the past month about what new features have been added to popular web browsers, "
        "how they are incorporating AI, and how they are navigating challenges like the windows 10 transition. Then summarize these findings."
        "Firefox should be omitted from this search as we are focusing on Firefox's competitors."
    )
    resp1 = client.responses.create(
        model="gpt-4o-mini",
        tools=[{"type": "web_search_preview"}],
        instructions="Generate a markdown formatted response, with a H2 header title",
        input=prompt1,
    )

    final_output_1 += (
        f"**Question:**\n{prompt1}\n\n" f"**Answer:**\n{resp1.output_text}\n\n"
    )
    response_object_1 = json.dumps(resp1.to_dict(), indent=2)

    final_report += f"\n{resp1.output_text}\n\n"

    # Ask ChatGPT to summarize scraped chrome release notes
    prompt2 = """What new features has Chrome been working on recently?
    Please include the release number you found these features in and the date of that release."""
    resp2 = client.responses.create(
        model="gpt-4o-mini",
        instructions="Generate a markdown formatted response, with a H2 header title",
        input=[
            {"role": "system", "content": "You are an expert summarizer."},
            {"role": "user", "content": prompt2},
            {"role": "user", "content": file_contents2},
        ],
    )

    final_output_2 += (
        f"**Question:**\n{prompt2}\n\n" f"**Answer:**\n{resp2.output_text}\n\n"
    )
    response_object_2 = json.dumps(resp2.to_dict(), indent=2)

    final_report += f"\n\n{resp2.output_text}\n\nMore details can be found here: [Chrome Release Notes](https://developer.chrome.com/release-notes)"

    # Ask ChatGPT to summarize AI features in Chrome
    prompt3 = "What AI features has Chrome been working on recently?"
    resp3 = client.responses.create(
        model="gpt-4o-mini",
        instructions="Generate a markdown formatted response, with a H2 header title.",
        input=[
            {"role": "system", "content": "You are an expert summarizer."},
            {"role": "user", "content": prompt3},
            {"role": "user", "content": file_contents3},
        ],
    )

    final_output_3 += (
        f"**Question:**\n{prompt3}\n\n" f"**Answer:**\n{resp3.output_text}\n\n"
    )
    response_object_3 = json.dumps(resp3.to_dict(), indent=2)

    final_report += f"\n\n{resp3.output_text}\n\nMore details can be found here: [AI with Chrome](https://developer.chrome.com/docs/ai)"

    # Ask ChatGPT to summarize recent Chrome Dev Tools News
    prompt4 = "What new features are available in Chrome Dev Tools?"
    resp4 = client.responses.create(
        model="gpt-4o-mini",
        instructions="Generate a markdown formatted response, with a H2 header title",
        input=[
            {"role": "system", "content": "You are an expert summarizer."},
            {"role": "user", "content": prompt4},
            {"role": "user", "content": file_contents4},
        ],
    )

    final_output_4 += (
        f"**Question:**\n{prompt4}\n\n" f"**Answer:**\n{resp4.output_text}\n\n"
    )
    response_object_4 = json.dumps(resp4.to_dict(), indent=2)

    final_report += f"\n\n{resp4.output_text}\n\nMore details can be found here: [Chrome Dev Tools](https://developer.chrome.com/docs/devtools/news?hl=en#whats-new)"

    # Ask ChatGPT to find browser & device partnership news
    prompt5 = """Please find articles related to browser & device partnerships.
Firefox should be omitted from this search as we are focusing on Firefox's competitors.
Please find all recent announcements of browser-device partnerships with browsers like Chrome, Edge, Safari, etc. and
devices like mobile phones, Smart TVs, or VR (virtual reality), then summarize these findings."""
    resp5 = client.responses.create(
        model="gpt-4o-mini",
        tools=[{"type": "web_search_preview"}],
        instructions="You are an expert summarizer.  Generate a markdown formatted response, with a H2 header title",
        input=prompt5,
    )

    final_output_5 += (
        f"**Question:**\n{prompt5}\n\n" f"**Answer:**\n{resp5.output_text}\n\n"
    )
    response_object_5 = json.dumps(resp5.to_dict(), indent=2)

    final_report += f"\n{resp5.output_text}\n\n"

    # Ask ChatGPT to find news in the online ads space
    prompt6 = "Look for articles from the past month about news related to online advertising, and summarize the findings."
    resp6 = client.responses.create(
        model="gpt-4o-mini",
        tools=[{"type": "web_search_preview"}],
        instructions="Generate a markdown formatted response, with a H2 header title",
        input=prompt6,
    )

    final_output_6 += (
        f"**Question:**\n{prompt6}\n\n" f"**Answer:**\n{resp6.output_text}\n\n"
    )
    response_object_6 = json.dumps(resp6.to_dict(), indent=2)

    final_report += f"\n{resp6.output_text}\n\n"

    # Ask ChatGPT for news about the AI space in General
    prompt7 = (
        "Look for articles from the past month about news related to AI in general."
    )
    resp7 = client.responses.create(
        model="gpt-4o-mini",
        tools=[{"type": "web_search_preview"}],
        instructions="Generate a markdown formatted response, with a H2 header title",
        input=prompt7,
    )

    final_output_7 += (
        f"**Question:**\n{prompt5}\n\n" f"**Answer:**\n{resp5.output_text}\n\n"
    )
    response_object_7 = json.dumps(resp5.to_dict(), indent=2)

    final_report += f"\n{resp7.output_text}\n\n"

    # Ask ChatGPT which upcoming holidays/events may impact DAU?
    prompt8 = "What holidays or events that could potentially impact browser usage are coming up in the next few months?"
    resp8 = client.responses.create(
        model="gpt-4o-mini",
        tools=[{"type": "web_search_preview"}],
        instructions="Generate a markdown formatted response, with a H2 header title",
        input=prompt8,
    )

    final_output_8 += (
        f"**Question:**\n{prompt8}\n\n" f"**Answer:**\n{resp8.output_text}\n\n"
    )
    response_object_8 = json.dumps(resp8.to_dict(), indent=2)

    final_report += f"\n{resp8.output_text}\n\n"

    # Make the output fpaths for storing the full responses received from ChatGPT
    response_fpath1 = OUTPUT_FPATH_1 + resp1.id + logical_dag_date_str + ".json"
    response_fpath2 = OUTPUT_FPATH_2 + resp2.id + logical_dag_date_str + ".json"
    response_fpath3 = OUTPUT_FPATH_3 + resp3.id + logical_dag_date_str + ".json"
    response_fpath4 = OUTPUT_FPATH_4 + resp4.id + logical_dag_date_str + ".json"
    response_fpath5 = OUTPUT_FPATH_5 + resp5.id + logical_dag_date_str + ".json"
    response_fpath6 = OUTPUT_FPATH_6 + resp6.id + logical_dag_date_str + ".json"
    response_fpath7 = OUTPUT_FPATH_7 + resp7.id + logical_dag_date_str + ".json"
    response_fpath8 = OUTPUT_FPATH_8 + resp8.id + logical_dag_date_str + ".json"

    # Save all summaries to GCS as an intermediate step
    client = storage.Client(project="moz-fx-data-shared-prod")
    bucket = client.bucket(BUCKET_NO_GS)

    blob1 = bucket.blob(final_output_fpath1)
    blob1.upload_from_string(final_output_1)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath1}")
    blob1_metadata = bucket.blob(response_fpath1)
    blob1_metadata.upload_from_string(response_object_1)
    print(f"Full api response uploaded to gs://{BUCKET_NO_GS}/{response_fpath1}")

    blob2 = bucket.blob(final_output_fpath2)
    blob2.upload_from_string(final_output_2)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath2}")
    blob2_metadata = bucket.blob(response_fpath2)
    blob2_metadata.upload_from_string(response_object_2)
    print(f"Full api response uploaded to gs://{BUCKET_NO_GS}/{response_fpath2}")

    blob3 = bucket.blob(final_output_fpath3)
    blob3.upload_from_string(final_output_3)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath3}")
    blob3_metadata = bucket.blob(response_fpath3)
    blob3_metadata.upload_from_string(response_object_3)
    print(f"Full api response uploaded to gs://{BUCKET_NO_GS}/{response_fpath3}")

    blob4 = bucket.blob(final_output_fpath4)
    blob4.upload_from_string(final_output_4)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath4}")
    blob4_metadata = bucket.blob(response_fpath4)
    blob4_metadata.upload_from_string(response_object_4)
    print(f"Full api response uploaded to gs://{BUCKET_NO_GS}/{response_fpath4}")

    blob5 = bucket.blob(final_output_fpath5)
    blob5.upload_from_string(final_output_5)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath5}")
    blob5_metadata = bucket.blob(response_fpath5)
    blob5_metadata.upload_from_string(response_object_5)
    print(f"Full api response uploaded to gs://{BUCKET_NO_GS}/{response_fpath5}")

    blob6 = bucket.blob(final_output_fpath6)
    blob6.upload_from_string(final_output_6)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath6}")
    blob6_metadata = bucket.blob(response_fpath6)
    blob6_metadata.upload_from_string(response_object_6)
    print(f"Full api response uploaded to gs://{BUCKET_NO_GS}/{response_fpath6}")

    blob7 = bucket.blob(final_output_fpath7)
    blob7.upload_from_string(final_output_7)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath7}")
    blob7_metadata = bucket.blob(response_fpath7)
    blob7_metadata.upload_from_string(response_object_7)
    print(f"Full api response uploaded to gs://{BUCKET_NO_GS}/{response_fpath7}")

    blob8 = bucket.blob(final_output_fpath8)
    blob8.upload_from_string(final_output_8)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath8}")
    blob8_metadata = bucket.blob(response_fpath8)
    blob8_metadata.upload_from_string(response_object_8)
    print(f"Full api response uploaded to gs://{BUCKET_NO_GS}/{response_fpath8}")

    final_report_path = FINAL_REPORT_FPATH + report_date + ".md"
    final_blob = bucket.blob(final_report_path)
    final_blob.upload_from_string(final_report)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_report_path}")

    # TODO
    # Build the message & include the link to the report
    # message = {"text": "Hello, here is the link to the report!"}

    # Send the message & report to Slack
    # requests.post(SLACK_WEBHOOK, json=message)


if __name__ == "__main__":
    main()
