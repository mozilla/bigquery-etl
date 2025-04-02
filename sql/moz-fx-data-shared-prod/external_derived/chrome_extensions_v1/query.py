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


def get_website_url_from_soup(webpage_soup):
    """Input: Webpage Soup
    Output: Website URL (str) if found, otherwise return None """
    website_url = None
    website_links = webpage_soup.find_all("a")
    for website_link in website_links:
        if website_link.has_attr("href") and "Website" in website_link.text:
            website_url = website_link["href"]
    return website_url


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
            "extension_updated_date",
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
    number_of_ratings = None
    chrome_extension_name = None
    star_rating = None
    number_of_users = None
    extension_version = None
    extension_size = None
    extension_languages = None
    developer_desc = None
    developer_email = None
    developer_website = None
    developer_phone = None
    extension_updated_date = None

    # Get the soup from the current link
    current_link_soup = get_soup_from_webpage(
        webpage_url=url, timeout_seconds=timeout_limit
    )

    # Get paragraphs & headers from the current link
    paragraphs_from_current_link_soup = get_paragraphs_from_soup(current_link_soup)
    headers_from_current_link_soup = get_h1_headers_from_soup(current_link_soup)
    h2_headers_from_current_link_soup = get_h2_headers_from_soup(current_link_soup)
    divs_from_current_link_soup = get_divs_from_soup(current_link_soup)

    # Get the developer website URL
    developer_website = get_website_url_from_soup(current_link_soup)

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

    # Loop through the divs
    for div in divs_from_current_link_soup:
        # Get the number of users
        if " users" in div:
            pattern = r"(\d{1,3}(?:,\d{3})*|\d+) (?=users)"
            match = re.search(pattern, div)
            if match:
                number_of_users = match.group(0).split(" ")[0].replace(",", "")

    # Loop through divs
    for index, div in enumerate(divs_from_current_link_soup):
        # If you see updated in div and it's not the last div found
        if "Updated" in div and index + 1 < len(divs_from_current_link_soup):
            # The next div should have the extension_updated_date
            extension_updated_date = divs_from_current_link_soup[index + 1]
        if "Version" in div and index + 1 < len(divs_from_current_link_soup):
            # The next div should have the extension version
            extension_version = divs_from_current_link_soup[index + 1]
        if "Size" in div and index + 1 < len(divs_from_current_link_soup):
            # The next div should have the extension size
            extension_size = divs_from_current_link_soup[index + 1]
        if "Languages" in div and index + 1 < len(divs_from_current_link_soup):
            # The next div should have language info
            extension_languages = divs_from_current_link_soup[index + 1]

    # Get all divs
    all_divs = current_link_soup.find_all("div")
    # Loop through each div
    for idx, div in enumerate(all_divs):
        developer_info = None
        # If the div is developer and it's not the last div
        if div.text.strip() == "Developer" and idx + 1 < len(all_divs):
            # Get the next div after Developer
            next_div = all_divs[idx + 1]
            # Find the first nested tag
            first_nested_tag = next_div.find()
            # If there is a first nested tag
            if first_nested_tag:
                # Get developer info as all the text from that first nested tag
                developer_info = first_nested_tag.get_text(separator="\n", strip=True)

                # If website is in developer info
                if "Website" in developer_info:
                    # Split on website and take the part before website as the developer description
                    developer_desc = developer_info.split("Website")[0].replace(
                        "\n", " "
                    )

                # If email is in developer info
                if "Email" in developer_info:
                    developer_email_and_phone = (
                        developer_info.split("Email")[1].replace("\n", " ").strip()
                    )
                    # If phone is there, get developer email and phone
                    if "Phone" in developer_email_and_phone:
                        developer_email_and_phone_list = (
                            developer_email_and_phone.split("Phone")
                        )
                        developer_email = developer_email_and_phone_list[0]
                        developer_phone = developer_email_and_phone_list[1]
                    # If phone is not there, only get developer email
                    else:
                        developer_email = developer_email_and_phone

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
            "extension_updated_date": [extension_updated_date],
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
                {"name": "extension_version", "type": "STRING", "mode": "NULLABLE"},
                {"name": "extension_size", "type": "STRING", "mode": "NULLABLE"},
                {"name": "extension_languages", "type": "STRING", "mode": "NULLABLE"},
                {"name": "developer_desc", "type": "STRING", "mode": "NULLABLE"},
                {"name": "developer_email", "type": "STRING", "mode": "NULLABLE"},
                {"name": "developer_website", "type": "STRING", "mode": "NULLABLE"},
                {"name": "developer_phone", "type": "STRING", "mode": "NULLABLE"},
                {
                    "name": "extension_updated_date",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_csv_to_gcp_job.result()


if __name__ == "__main__":
    main()
