# Load libraries
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
from argparse import ArgumentParser
from google.cloud import bigquery
from urllib.parse import urljoin

# Main website for Chrome Webstore
CHROME_WEBSTORE_URL = "https://chromewebstore.google.com"

# Links to ignore from the Chrome Webstore page
LIST_OF_LINKS_TO_IGNORE = [
    "https://chromewebstore.google.com/user/installed",
    "https://chrome.google.com/webstore/devconsole/",
    "https://www.google.com/intl/en/about/products",
    "https://support.google.com/chrome_webstore/answer/1047776?hl=en-US",
    "https://myaccount.google.com/privacypolicy?hl=en-US",
    "https://support.google.com/chrome_webstore?hl=en-US",
    "https://ssl.gstatic.com/chrome/webstore/intl/en-US/gallery_tos.html",
    "./",
]


TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.chrome_extensions_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
RESULTS_FPATH = "CHROME_EXTENSIONS/chrome_extensions_%s.csv"
TIMEOUT_IN_SECONDS = 10


# Function to get all unique links found on a given webpage
def get_unique_links_from_webpage(
    url, base_url, timeout_seconds, links_to_ignore, links_to_not_process
):
    """Input: Webpage, timeout seconds (integer), links to ignore, links already processed
    Output: List of unique, absolute links on that page"""

    response = requests.get(url, timeout=timeout_seconds)
    soup = BeautifulSoup(response.text, "html.parser")

    links = [urljoin(base_url, a["href"]) for a in soup.find_all("a", href=True)]
    unique_links = []
    for link in links:
        if (
            link not in unique_links
            and link not in links_to_ignore
            and link not in links_to_not_process
        ):
            unique_links.append(link)
    return unique_links


# Function to get the soup returned from a given page
def get_soup_from_webpage(webpage_url, timeout_seconds):
    """Input: Webpage URL, timeout
    Output: BeautifulSoup class"""
    response = requests.get(webpage_url, timeout=timeout_seconds)
    soup_from_webpage = BeautifulSoup(response.text, "html.parser")
    return soup_from_webpage


# Function to get all paragraphs from a soup
def get_paragraphs_from_soup(webpage_soup):
    """Input: Webpage Soup
    Output: List of paragraphs found in the soup"""
    paragraphs = [p.text for p in webpage_soup.find_all("p")]
    return paragraphs


def get_h1_headers_from_soup(webpage_soup):
    """Input: Webpage Soup
    Output: List of H1 Elements Found"""
    headers = [h1.text for h1 in webpage_soup.find_all("h1")]
    return headers


def get_h2_headers_from_soup(webpage_soup):
    """Input: Webpage Soup
    Output: List of H2 Elements Found
    """
    headers = [h2.text for h2 in webpage_soup.find_all("h2")]
    return headers


def get_divs_from_soup(webpage_soup):
    """Input: Webpage Soup
    Output: List of H2 Elements Found
    """
    divs = [div.text for div in webpage_soup.find_all("div")]
    return divs


def initialize_results_df():
    """Returns a dataframe with 0 rows with the desired format"""
    results_df = pd.DataFrame(
        columns=[
            "submission_date",
            "url",
            "chrome_extension_name",
            "star_rating",
            "number_of_ratings",
            "number_of_users",
            "extension_version",
            "extension_size",
            "extension_languages",
            "developer_desc",
            "developer_email",
            "developer_website",
            "developer_phone",
            "extension_updated_date"
        ]
    )
    return results_df


def check_if_detail_or_non_detail_page(url):
    """Input: A URL (string)
    Output: Boolean, indicating if the URL contains /detail/ in it"""
    detail_page = False
    if "/detail/" in url:
        detail_page = True
    return detail_page


def pull_data_from_detail_page(url, timeout_limit, current_date):
    """Input: URL, timeout limit (integer), and current date"""
    # Initialize as empty strings
    number_of_ratings = "NOT FOUND"
    chrome_extension_name = "NOT FOUND"
    star_rating = "NOT FOUND"
    number_of_users = "NOT FOUND"
    extension_version = "NOT FOUND"
    extension_size = "NOT FOUND"
    extension_languages = "NOT FOUND"
    developer_desc = "NOT FOUND"
    developer_email = "NOT FOUND"
    developer_website = "NOT FOUND"
    developer_phone = "NOT FOUND"
    extension_updated_date = "NOT FOUND"

    # Get the soup from the current link
    current_link_soup = get_soup_from_webpage(
        webpage_url=url, timeout_seconds=timeout_limit
    )

    # Get paragraphs & headers from the current link
    paragraphs_from_current_link_soup = get_paragraphs_from_soup(current_link_soup)
    headers_from_current_link_soup = get_h1_headers_from_soup(current_link_soup)
    h2_headers_from_current_link_soup = get_h2_headers_from_soup(current_link_soup)
    divs_from_current_link_soup = get_divs_from_soup(current_link_soup)

    # Get the number of ratings
    for paragraph in paragraphs_from_current_link_soup:
        # Check if this has the ratings information
        if paragraph.endswith("ratings"):
            # Get portion before the space
            number_of_ratings = paragraph.split(" ")[0]

    # Get the extension name
    for header in headers_from_current_link_soup:
        chrome_extension_name = header

    # Get the star rating
    for h2_header in h2_headers_from_current_link_soup:
        if "out of 5" in h2_header:
            pattern = r"^.*?out of 5"
            match = re.search(pattern, h2_header)
            if match:
                star_rating = match.group(0).split(" ")[0]

    # Get the number of users
    for div in divs_from_current_link_soup:
        if " users" in div:
            pattern = r"(\d{1,3}(?:,\d{3})*|\d+) (?=users)"
            match = re.search(pattern, div)
            if match:
                number_of_users = match.group(0).split(" ")[0].replace(",", "")

    # Put the results into a dataframe
    curr_link_results_df = pd.DataFrame(
        {
            "submission_date": [current_date],
            "url": [url],
            "chrome_extension_name": [chrome_extension_name],
            "star_rating": [star_rating],
            "number_of_ratings": [number_of_ratings],
            "number_of_users": [number_of_users],
            "extension_version": [extension_version],
            "extension_size": [extension_size],
            "extension_languages": [extension_languages],
            "developer_desc": [developer_desc],
            "developer_email": [developer_email],
            "developer_website": [developer_website],
            "developer_phone": [developer_phone],
            "extension_updated_date": [extension_updated_date]
        }
    )

    return curr_link_results_df


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    # Get DAG logical date
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    logical_dag_date_string = logical_dag_date.strftime("%Y-%m-%d")

    # Initialize a list of the non-detail page links that have already been processed
    links_already_processed = []

    # First, get a list of all the unique links from the Chrome webstore page
    # Excluding those on the list of links to ignore list
    unique_links_on_chrome_webstore_page = get_unique_links_from_webpage(
        url=CHROME_WEBSTORE_URL,
        base_url=CHROME_WEBSTORE_URL,
        timeout_seconds=TIMEOUT_IN_SECONDS,
        links_to_ignore=LIST_OF_LINKS_TO_IGNORE,
        links_to_not_process=links_already_processed,
    )

    # Initialize a dataframe to store extension level results
    results_df = initialize_results_df()

    # Loop through the links found on the main page of the Chrome Webstore
    for idx, current_link in enumerate(unique_links_on_chrome_webstore_page):

        # Check if the link is a "detail page" or a "non detail page"
        is_detail_page = check_if_detail_or_non_detail_page(current_link)

        # If the link is a detail page
        if is_detail_page:
            # Get the data from that page
            detail_page_results_df = pull_data_from_detail_page(
                url=current_link,
                timeout_limit=TIMEOUT_IN_SECONDS,
                current_date=logical_dag_date_string,
            )

            # Append the data scraped to results_df
            results_df = pd.concat([results_df, detail_page_results_df])

        # If this link is not a detail page
        else:
            # Get the links on this page
            unique_links_on_non_detail_page = get_unique_links_from_webpage(
                url=current_link,
                base_url=CHROME_WEBSTORE_URL,
                timeout_seconds=TIMEOUT_IN_SECONDS,
                links_to_ignore=LIST_OF_LINKS_TO_IGNORE,
                links_to_not_process=links_already_processed,
            )

            # Loop through each link on this page
            for link_on_page in unique_links_on_non_detail_page:

                # Check if it's a detail page link or not
                is_detail_page = check_if_detail_or_non_detail_page(link_on_page)

                # If it's a detail page, record results
                if is_detail_page:
                    # Get the data from that page
                    detail_page_results_df = pull_data_from_detail_page(
                        url=link_on_page,
                        timeout_limit=TIMEOUT_IN_SECONDS,
                        current_date=logical_dag_date_string,
                    )
                    # Append the data scraped to results_df
                    results_df = pd.concat([results_df, detail_page_results_df])

                # If it's another page with more links, ignore for now - we are only scraping 1 layer deep
                # Add the link on the already processed list
                links_already_processed.append(link_on_page)

        # Add the top level link to already processed
        links_already_processed.append(current_link)

    # Remove duplicates
    results_df = results_df.drop_duplicates()

    # Write data to CSV in GCS
    final_results_fpath = GCS_BUCKET + RESULTS_FPATH % (logical_dag_date_string)
    results_df.to_csv(final_results_fpath, index=False)
    print("Results written to: ", str(final_results_fpath))

    # Write data to BQ table
    # Open a connection to BQ
    client = bigquery.Client(TARGET_PROJECT)

    # If data already ran for this date, delete out
    delete_query = f"""DELETE FROM `moz-fx-data-shared-prod.external_derived.chrome_extensions_v1`
  WHERE submission_date = '{logical_dag_date_string}'"""
    del_job = client.query(delete_query)
    del_job.result()

    # Load data from GCS to BQ table - appending to what is already there
    load_csv_to_gcp_job = client.load_table_from_uri(
        final_results_fpath,
        TARGET_TABLE,
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_APPEND",
            schema=[
                {"name": "submission_date", "type": "DATE", "mode": "NULLABLE"},
                {"name": "url", "type": "STRING", "mode": "NULLABLE"},
                {"name": "chrome_extension_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "star_rating", "type": "NUMERIC", "mode": "NULLABLE"},
                {"name": "number_of_ratings", "type": "STRING", "mode": "NULLABLE"},
                {"name": "number_of_users", "type": "STRING", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_csv_to_gcp_job.result()


if __name__ == "__main__":
    main()
