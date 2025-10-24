"""Summarize market news using OpenAI and write results back to GCS."""

# Load libraries
import json
import os
import unicodedata

# import requests #Only needed once Slack added
import sys
from argparse import ArgumentParser
from datetime import datetime, date

from google.cloud import storage
from openai import OpenAI

# Set variables
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
BUCKET_NO_GS = "moz-fx-data-prod-external-data"
INSTRUCTIONS = "Generate a markdown formatted response, with a H2 header title"
MODEL_TYPE = "gpt-4o-mini"
SCRAPED_BASE = "MARKET_RESEARCH/SCRAPED_INFO/"
OUTPUT_BASE = "MARKET_RESEARCH/SUMMARY_INFO/"

# Filepath for the final, consolidated report
FINAL_REPORT_FPATH = "MARKET_RESEARCH/FINAL_REPORTS/MarketIntelBotReport_"

# Filepaths to read the data loaded to GCS by the "release_scraping DAG"
INPUT_FPATH_2 = SCRAPED_BASE + "ChromeReleaseNotes/WebScraping_"
INPUT_FPATH_3 = SCRAPED_BASE + "ChromeAI/WebScraping_"
INPUT_FPATH_4 = SCRAPED_BASE + "ChromeDevTools/WebScraping_"

# Filepaths to save ChatGPT Summaries to
OUTPUT_FPATH_1 = OUTPUT_BASE + "BrowserDevelopment/WebScraping_"
OUTPUT_FPATH_2 = OUTPUT_BASE + "ChromeReleaseNotes/WebScraping_"
OUTPUT_FPATH_3 = OUTPUT_BASE + "ChromeAI/WebScraping_"
OUTPUT_FPATH_4 = OUTPUT_BASE + "ChromeDevTools/WebScraping_"
OUTPUT_FPATH_5 = OUTPUT_BASE + "DeviceMarketResearch/WebScraping_"
OUTPUT_FPATH_6 = OUTPUT_BASE + "OnlineAdvertising/WebScraping_"
OUTPUT_FPATH_7 = OUTPUT_BASE + "AI_News/WebScraping_"
OUTPUT_FPATH_8 = OUTPUT_BASE + "UpcomingHolidays/WebScraping_"

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


def summarize_with_open_ai(
    client, model, instructions, prompt, input_text, use_web_tool
):
    """Summarize with open ai."""
    if use_web_tool:
        resp = client.responses.create(
            model=model,
            tools=[{"type": "web_search_preview"}],
            instructions=instructions,
            input=prompt,
        )
    else:
        convo = [
            {"role": "system", "content": "You are an expert summarizer."},
            {"role": "user", "content": prompt},
        ]
        if input_text:
            convo.append({"role": "user", "content": input_text})
        resp = client.responses.create(
            model=model,
            instructions=instructions,
            input=convo,
        )
    response_output_text = resp.output_text
    response_output_text = unicodedata.normalize("NFKC", response_output_text)
    return response_output_text, json.dumps(resp.to_dict(), indent=2)


def write_to_gcs(
    bucket, final_output_fpath, final_output, response_fpath, response_object
):
    blob = bucket.blob(final_output_fpath)
    blob.upload_from_string(final_output)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath}")
    blob8_metadata = bucket.blob(response_fpath)
    blob8_metadata.upload_from_string(response_object)
    print(f"Full api response uploaded to gs://{BUCKET_NO_GS}/{response_fpath}")


def create_intmd_fpath(output_fpath, logical_dag_date_str):
    """Generate intermediate fpath string."""
    return output_fpath + logical_dag_date_str + ".md"


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
    final_output_fpath1 = create_intmd_fpath(OUTPUT_FPATH_1, logical_dag_date_str)
    final_output_fpath2 = create_intmd_fpath(OUTPUT_FPATH_2, logical_dag_date_str)
    final_output_fpath3 = create_intmd_fpath(OUTPUT_FPATH_3, logical_dag_date_str)
    final_output_fpath4 = create_intmd_fpath(OUTPUT_FPATH_4, logical_dag_date_str)
    final_output_fpath5 = create_intmd_fpath(OUTPUT_FPATH_5, logical_dag_date_str)
    final_output_fpath6 = create_intmd_fpath(OUTPUT_FPATH_6, logical_dag_date_str)
    final_output_fpath7 = create_intmd_fpath(OUTPUT_FPATH_7, logical_dag_date_str)
    final_output_fpath8 = create_intmd_fpath(OUTPUT_FPATH_8, logical_dag_date_str)

    # Make the output fpaths for storing the full responses received from ChatGPT
    response_fpath1 = OUTPUT_FPATH_1 + logical_dag_date_str + ".json"
    response_fpath2 = OUTPUT_FPATH_2 + logical_dag_date_str + ".json"
    response_fpath3 = OUTPUT_FPATH_3 + logical_dag_date_str + ".json"
    response_fpath4 = OUTPUT_FPATH_4 + logical_dag_date_str + ".json"
    response_fpath5 = OUTPUT_FPATH_5 + logical_dag_date_str + ".json"
    response_fpath6 = OUTPUT_FPATH_6 + logical_dag_date_str + ".json"
    response_fpath7 = OUTPUT_FPATH_7 + logical_dag_date_str + ".json"
    response_fpath8 = OUTPUT_FPATH_8 + logical_dag_date_str + ".json"

    # Read in the scraped data from the 2 files
    file_contents2 = read_gcs_file(gcs_fpath2)
    file_contents3 = read_gcs_file(gcs_fpath3)
    file_contents4 = read_gcs_file(gcs_fpath4)

    # Initialize the final output as an empty string
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

    # Prompt #1 - New Features in Popular Browsers
    prompt1 = (
        "Look for articles from the past month about what new features have been added to popular web browsers, "
        "how they are incorporating AI, and how they are navigating challenges like the windows 10 transition. Then summarize these findings."
        "Firefox should be omitted from this search as we are focusing on Firefox's competitors."
    )
    final_output_1, response_object_1 = summarize_with_open_ai(
        client, MODEL_TYPE, INSTRUCTIONS, prompt1, input_text=None, use_web_tool=True
    )
    final_report += f"\n{final_output_1}\n\n"

    # Prompt #2 - New Features in Chrome
    prompt2 = """What new features has Chrome been working on recently?
    Please include the release number you found these features in and the date of that release."""
    final_output_2, response_object_2 = summarize_with_open_ai(
        client,
        MODEL_TYPE,
        INSTRUCTIONS,
        prompt2,
        input_text=file_contents2,
        use_web_tool=False,
    )
    final_report += f"\n\n{final_output_2}\n\nMore details can be found here: [Chrome Release Notes](https://developer.chrome.com/release-notes)"

    # Prompt #3 - New AI Features in Chrome
    prompt3 = "What AI features has Chrome been working on recently?"
    final_output_3, response_object_3 = summarize_with_open_ai(
        client,
        MODEL_TYPE,
        INSTRUCTIONS,
        prompt3,
        input_text=file_contents3,
        use_web_tool=False,
    )
    final_report += f"\n\n{final_output_3}\n\nMore details can be found here: [AI with Chrome](https://developer.chrome.com/docs/ai)"

    # Prompt #4 - New Features in Chrome Dev Tools
    prompt4 = "What new features are available in Chrome Dev Tools?"
    final_output_4, response_object_4 = summarize_with_open_ai(
        client,
        MODEL_TYPE,
        INSTRUCTIONS,
        prompt4,
        input_text=file_contents4,
        use_web_tool=False,
    )
    final_report += f"\n\n{final_output_4}\n\nMore details can be found here: [Chrome Dev Tools](https://developer.chrome.com/docs/devtools/news?hl=en#whats-new)"

    # Prompt #5 - Browser & Device Partnership News
    prompt5 = """Please find articles related to browser & device partnerships.
Firefox should be omitted from this search as we are focusing on Firefox's competitors.
Please find all recent announcements of browser-device partnerships with browsers like Chrome, Edge, Safari, etc. and
devices like mobile phones, Smart TVs, or VR (virtual reality), then summarize these findings."""
    final_output_5, response_object_5 = summarize_with_open_ai(
        client, MODEL_TYPE, INSTRUCTIONS, prompt5, input_text=None, use_web_tool=True
    )
    final_report += f"\n{final_output_5}\n\n"

    # Prompt #6 - Online Advertising News
    prompt6 = "Look for articles from the past month about news related to online advertising, and summarize the findings."
    final_output_6, response_object_6 = summarize_with_open_ai(
        client, MODEL_TYPE, INSTRUCTIONS, prompt6, input_text=None, use_web_tool=True
    )
    final_report += f"\n{final_output_6}\n\n"

    # Prompt #7 - AI News
    prompt7 = (
        "Look for articles from the past month about news related to AI in general."
    )
    final_output_7, response_object_7 = summarize_with_open_ai(
        client, MODEL_TYPE, INSTRUCTIONS, prompt7, input_text=None, use_web_tool=True
    )
    final_report += f"\n{final_output_7}\n\n"

    # Prompt #8 - Upcoming holidays/events that could impact browser usage
    prompt8 = "What holidays or events that could potentially impact browser usage are coming up in the next few months?"
    final_output_8, response_object_8 = summarize_with_open_ai(
        client, MODEL_TYPE, INSTRUCTIONS, prompt8, input_text=None, use_web_tool=True
    )
    final_report += f"\n{final_output_8}\n\n"

    # Save all summaries to GCS as an intermediate step
    client = storage.Client(project="moz-fx-data-shared-prod")
    bucket = client.bucket(BUCKET_NO_GS)

    write_to_gcs(
        bucket, final_output_fpath1, final_output_1, response_fpath1, response_object_1
    )
    write_to_gcs(
        bucket, final_output_fpath2, final_output_2, response_fpath2, response_object_2
    )
    write_to_gcs(
        bucket, final_output_fpath3, final_output_3, response_fpath3, response_object_3
    )
    write_to_gcs(
        bucket, final_output_fpath4, final_output_4, response_fpath4, response_object_4
    )
    write_to_gcs(
        bucket, final_output_fpath5, final_output_5, response_fpath5, response_object_5
    )
    write_to_gcs(
        bucket, final_output_fpath6, final_output_6, response_fpath6, response_object_6
    )
    write_to_gcs(
        bucket, final_output_fpath7, final_output_7, response_fpath7, response_object_7
    )
    write_to_gcs(
        bucket, final_output_fpath8, final_output_8, response_fpath8, response_object_8
    )

    # Save the final consolidated report to GCS
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
