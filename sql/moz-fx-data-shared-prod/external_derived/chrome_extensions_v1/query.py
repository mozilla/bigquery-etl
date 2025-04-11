# Load libraries
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
from argparse import ArgumentParser
from google.cloud import bigquery
from urllib.parse import urljoin
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# Main website for Chrome Webstore
CHROME_WEBSTORE_URL = "https://chromewebstore.google.com"

ADDITIONAL_LINK_TO_GRAB = "https://chromewebstore.google.com/category/extensions"

# Links to ignore from the Chrome Webstore page
LIST_OF_LINKS_TO_IGNORE = [
    "https://chromewebstore.google.com/user/installed",
    "https://chrome.google.com/webstore/devconsole/",
    "https://www.google.com/intl/en/about/products",
    "https://support.google.com/chrome_webstore/answer/1047776?hl=en-US",
    "https://myaccount.google.com/privacypolicy?hl=en-US",
    "https://support.google.com/chrome_webstore?hl=en-US",
    "https://accounts.google.com/TOS?loc=US&hl=en-US",
    "https://support.google.com/accounts?hl=en-US&p=account_iph",
    "https://support.google.com/accounts?p=signin_privatebrowsing&hl=en-US",
    "https://ssl.gstatic.com/chrome/webstore/intl/en-US/gallery_tos.html",
    "https://support.google.com/chrome_webstore/answer/12225786?p=cws_reviews_results&hl=en-US",
    "https://www.google.com/chrome/?brand=GGRF&utm_source=google.com&utm_medium=material-callout&utm_campaign=cws&utm_keyword=GGRF",
    "./",
]

# Define where to write the data in GCS and BQ and other variables
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.chrome_extensions_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
RESULTS_FPATH = "CHROME_EXTENSIONS/chrome_extensions_%s.csv"
TIMEOUT_IN_SECONDS = 10
MAX_CLICKS = 30  # Max # of times you want to click load more before stopping and recording results

# --------------DEFINE REUSABLE FUNCTIONS------------------------


def get_unique_links_from_webpage(url, base_url, links_to_ignore, links_to_not_process):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    driver.get(url)

    time.sleep(3)  # Wait for JS to load content

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

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


def get_unique_links_from_html(html, base_url, links_to_ignore, links_to_not_process):
    """Input: Raw HTML string, base URL, links to ignore, and links already processed
    Output: List of unique, absolute links on that page"""

    soup = BeautifulSoup(html, "html.parser")

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
    Output: Website URL (str) if found, otherwise return None"""
    website_url = None
    website_links = webpage_soup.find_all("a")
    for website_link in website_links:
        if website_link.has_attr("href") and "Website" in website_link.text:
            website_url = website_link["href"]
    return website_url


def get_category_from_soup(webpage_soup):
    """Input: Webpage Soup
    Output: Category of the extension if found, otherwise return None"""
    category = None
    website_links = webpage_soup.find_all("a")
    for link_nbr, website_link in enumerate(website_links):
        if (
            website_link.has_attr("href")
            and "Extension" in website_link.text
            and link_nbr + 1 < len(website_links)
        ):
            # Get the next link text
            category = website_links[link_nbr + 1].text.strip()

    return category


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
            "category",
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


def check_if_load_more_button_present(webpage_soup):
    """Input: Webpage Soup
    Output: Boolean indicating if load more button is on the page"""
    buttons = webpage_soup.find_all("button")
    for button in buttons:
        if "Load more" in button.get_text(strip=True):
            return True
    return False


def get_links_from_non_detail_page(
    url, list_of_links_already_processed, max_clicks, links_to_ignore_list
):

    # Initialize a driver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    wait = WebDriverWait(driver, 10)
    # Get the URL and wait 2 seconds
    driver.get(url)
    time.sleep(2)
    # Initialize click count to 0 clicks
    click_count = 0

    # Click "Load more" until it's gone or max clicks reached
    while click_count < max_clicks:
        try:
            load_more_button = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//button//span[contains(text(), "Load more")]')
                )
            )
            driver.execute_script("arguments[0].click();", load_more_button)
            print(f"[{click_count+1}] Clicked 'Load more'")
            time.sleep(2)
            click_count += 1
        except:
            print("No more 'Load more' button or timeout.")
            break

    # Scroll to bottom to trigger lazy loading
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(4)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # Get set of links already processed
    processed_set = set(list_of_links_already_processed)

    # Get all extension card links (those that have '/detail/' in href)
    all_links = driver.find_elements(By.TAG_NAME, "a")
    extension_links = [
        link.get_attribute("href")
        for link in all_links
        if link.get_attribute("href") and "/detail/" in link.get_attribute("href")
    ]

    unique_extension_links = list(set(extension_links) - processed_set)

    all_href_links = [
        link.get_attribute("href") for link in all_links if link.get_attribute("href")
    ]

    unique_all_links = set(all_href_links)
    unique_non_extension_links = list(
        unique_all_links
        - set(unique_extension_links)
        - processed_set
        - set(links_to_ignore_list)
    )

    driver.quit()

    return unique_extension_links, unique_non_extension_links


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

    category = get_category_from_soup(current_link_soup)

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
            "category": [category],
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

    # Initialize an empty set to hold links that have already been processed
    links_already_processed = set()

    # Get all unique links found on the CHROME_WEBSTORE_URL (excluding links to ignore)
    unique_links_on_chrome_webstore_page = get_unique_links_from_webpage(
        url=CHROME_WEBSTORE_URL,
        base_url=CHROME_WEBSTORE_URL,
        links_to_ignore=LIST_OF_LINKS_TO_IGNORE,
        links_to_not_process=links_already_processed,
    )
    # Add on the additional link to grab
    unique_links_on_chrome_webstore_page.append(ADDITIONAL_LINK_TO_GRAB)

    # Add CHROME_WEBSTORE_URL to list of already processed so we don't process it again
    links_already_processed.add(CHROME_WEBSTORE_URL)
    links_already_processed.add(CHROME_WEBSTORE_URL + "/")

    # If the chrome webstore URL is in there again but just with a slash on the end, remove that from the list as well
    unique_links_on_chrome_webstore_page.remove(CHROME_WEBSTORE_URL + "/")
    print(len(unique_links_on_chrome_webstore_page))

    # Get the final cleaned up list of links on the main page we want to process, excluding anything that starts with accounts
    main_page_links_to_process = []

    for link in unique_links_on_chrome_webstore_page:
        if not link.startswith("https://accounts.google.com"):
            main_page_links_to_process.append(link)

    del unique_links_on_chrome_webstore_page

    # Initialize a dataframe to store extension level results
    results_df = initialize_results_df()

    # Loop through the links found on the main page of the Chrome Webstore
    for idx, current_link in enumerate(main_page_links_to_process):
        print("Currently processing link: ", current_link)

        percent_done = (idx + 1) / len(main_page_links_to_process) * 100
        if idx % 5 == 0 or idx == len(main_page_links_to_process) - 1:
            print(
                f"Progress: {percent_done:.1f}% ({idx + 1} of {len(main_page_links_to_process)})"
            )

        # Check if the link is a "detail page" or a "non detail page"
        is_detail_page = check_if_detail_or_non_detail_page(current_link)

        # If the link is a detail page and not already processed
        if is_detail_page:
            print("link is a detail page")
            if current_link not in links_already_processed:
                print("link is not yet processed, pulling data...")
                # Get the data from that page
                detail_page_results_df = pull_data_from_detail_page(
                    url=current_link,
                    timeout_limit=TIMEOUT_IN_SECONDS,
                    current_date=logical_dag_date_string,
                )

                # Append the data scraped to results_df
                results_df = pd.concat([results_df, detail_page_results_df])
                print("Added data to results_df")

                # Add the detail page link to links already processed
                links_already_processed.add(current_link)
            else:
                print("link is already processed")

        # If this link is not a detail page
        else:
            print("Link is not a detail page.")
            # Get the HTML from the non detail page after clicking load more button a bunch if present and scrolling each time
            # Also update links already processed
            print("Getting links from the non detail page...")
            detail_links_found, non_detail_links_found = get_links_from_non_detail_page(
                current_link,
                links_already_processed,
                MAX_CLICKS,
                LIST_OF_LINKS_TO_IGNORE,
            )

            # Print # of detail and non detail links found
            print("# detail links found on page: ", str(len(detail_links_found)))
            print("# non detail links found: ", str(len(non_detail_links_found)))

            # Loop through each link on this page
            print("Looping through detail links found...")
            for detail_link in detail_links_found:
                print("Processing detail link: ", detail_link)

                try:
                    # Get the data from that page
                    detail_page_results_df = pull_data_from_detail_page(
                        url=detail_link,
                        timeout_limit=TIMEOUT_IN_SECONDS,
                        current_date=logical_dag_date_string,
                    )
                    # Append the data scraped to results_df
                    results_df = pd.concat([results_df, detail_page_results_df])

                except Exception as e:
                    print(f"Failed to process detail page: {link_on_page}")
                    print("Error:", e)
                links_already_processed.add(detail_link)
            print("Done looping through detail links found.")

            # Loop through all the non detail links found
            print("Looping through non detail links found...")
            for non_detail_link in non_detail_links_found:
                print("Current non detail link: ", non_detail_link)
                if non_detail_link in links_already_processed:
                    print("Already processed, not processing again")
                else:
                    print("Processing non_detail_link: ", non_detail_link)
                    # Try again to get the detail links
                    next_level_detail_links_found, next_level_non_detail_links_found = (
                        get_links_from_non_detail_page(
                            non_detail_link,
                            links_already_processed,
                            MAX_CLICKS,
                            LIST_OF_LINKS_TO_IGNORE,
                        )
                    )

                    for next_level_detail_link_found in next_level_detail_links_found:
                        try:
                            # Get the data from that page
                            detail_page_results_df = pull_data_from_detail_page(
                                url=next_level_detail_link_found,
                                timeout_limit=TIMEOUT_IN_SECONDS,
                                current_date=logical_dag_date_string,
                            )
                            # Append the data scraped to results_df
                            results_df = pd.concat([results_df, detail_page_results_df])
                        except Exception as e:
                            print(
                                f"Failed to process detail page: {next_level_detail_link_found}"
                            )
                            print("Error:", e)

                        links_already_processed.add(next_level_detail_link_found)

                    for (
                        next_level_non_detail_link_found
                    ) in next_level_non_detail_links_found:
                        print("Not processing: ", next_level_non_detail_link_found)
                        print(
                            "This is because I only built it to go a few levels deep so far"
                        )

    # Remove duplicates
    results_df = results_df.drop_duplicates()

    # Output summary
    print(
        f"Scraped {len(results_df)} rows in total from {len(links_already_processed)} pages."
    )

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
                {
                    "name": "category",
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