import datetime
import json
import shutil
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Tuple

import click
import requests


def get_country_info(country_code: str) -> Tuple[str, str]:
    """Return base url and cookie suffix for given country."""
    if country_code == "US":
        return "https://affiliate-program.amazon.com/home", "main"
    elif country_code == "UK":
        return "https://affiliate-program.amazon.co.uk/home", "acbuk"
    elif country_code == "DE":
        return "https://partnernet.amazon.de/home", "abcde"
    else:
        raise ValueError(f"Unrecognized country code: {country_code}")


def generate_reports(
    start_date: datetime.date,
    end_date: datetime.date,
    store_id: str,
    base_url: str,
    headers: Dict[str, str],
):
    """Make request to generate reports for date interval and return export id."""
    print(f"Generating reports for {start_date} to {end_date}")

    export_url = f"{base_url}/reports/export.json"

    export_body = {
        "query": {
            "fromDate": f"{start_date}T04:00:00.000Z",
            "toDate": f"{end_date}T04:00:00.000Z",
            "selectionType": "custom",
            "ext": "csv",
            "trackingId": "all",
            "orders": {"enabled": True},
            "earnings": {"enabled": True},
            "tracking": {"enabled": True},
            "linktype": {"enabled": True},
            "trends": {"enabled": True},
            "bounty": {"enabled": True},
            "earnings_hva": {"enabled": True},
            "orders_with_clicks": {"enabled": True},
            "start_date": str(start_date),
            "end_date": str(end_date),
            "cache_filter": "custom",
            "types": [
                "orders",
                "earnings",
                "tracking",
                "linktype",
                "trends",
                "bounty",
                "earnings_hva",
                "orderswithclicks",
            ],
            "orders_filter": {"tag_id": "all"},
            "earnings_filter": {"tag_id": "all"},
            "tracking_filter": {"tag_id": "all"},
            "linktype_filter": {"tag_id": "all"},
            "trends_filter": {"tag_id": "all"},
            "bounty_filter": {"tag_id": "all"},
            "orders_with_clicks_filter": {"tag_id": "all"},
        },
        "store_id": store_id,
    }

    export_response = requests.post(
        export_url,
        data=json.dumps(export_body),
        headers=headers,
    )
    export_response.raise_for_status()

    # export response is an array of all available reports
    report_list = sorted(
        export_response.json(),
        key=lambda item: item["scheduled_timestamp"],
        reverse=True,
    )
    return report_list[0]["request_id"]  # export id


def check_report_status(
    store_id: str, base_url: str, headers: Dict[str, str], export_id: str
) -> List[Dict[str, Any]]:
    """Return a list of reports if finished generating, otherwise return empty list."""
    status_url = f"{base_url}/reports/export/status.json?store_id={store_id}"

    status_response = requests.get(status_url, headers=headers)
    status_response.raise_for_status()

    # status response is an array of all available reports
    report_list = list(
        filter(lambda report: report["request_id"] == export_id, status_response.json())
    )

    if len(report_list) == 0:
        raise ValueError(f"No reports found for request id {export_id}")

    exported_reports = list(
        filter(
            lambda report: report["status"] not in ["IN_QUEUE", "IN_PROGRESS"],
            report_list,
        )
    )

    print(f"Export {export_id}: {len(exported_reports)} out of {len(report_list)} complete")

    if len(exported_reports) == len(report_list):
        return exported_reports
    else:
        return []


def download_reports(
    report_list: List[Dict],
    filename_suffix: str,
    base_url: str,
    store_id: str,
    headers: Dict[str, str],
    output_dir: Path,
):
    """Download all available reports in the given list."""
    for report in report_list:
        remote_filename = report.get("file_path")
        if remote_filename is None:
            print(f"{report['report_name']} contains no data")
            continue

        local_filename = f"{report['report_name']}_{filename_suffix}.zip"

        print(f"Downloading {remote_filename} to {local_filename}")
        download_url = (
            f"{base_url}/reports/download?store_id={store_id}"
            f"&file={remote_filename}&name=a.zip"
        )

        with requests.get(download_url, headers=headers, stream=True) as response:
            with (output_dir / local_filename).open("wb") as f:
                shutil.copyfileobj(response.raw, f)


@click.command()
@click.option(
    "--start-date",
    required=True,
    type=datetime.date.fromisoformat,
    help="First date to export",
)
@click.option(
    "--end-date",
    required=True,
    type=datetime.date.fromisoformat,
    help="Last date to export",
)
@click.option(
    "--max-export-interval",
    type=int,
    default=21,
    help="Max number of days to include in a single file export (each file is limited to ~500k rows)",
)
@click.option("--cookies", required=True, help="Contents of cookie header")
@click.option("--csrf-token", required=True, help="X-CSRF-Token header value")
@click.option(
    "--country",
    required=True,
    type=click.Choice(["US", "UK", "DE"], case_sensitive=False),
    help="Country code of web portal",
)
@click.option("--store-id", required=True)
@click.option(
    "--output-dir",
    default=Path(__file__).parent / "amazon_downloads",
    type=Path,
    help="Directory to put downloaded file in",
)
def fetch_reports(
    start_date,
    end_date,
    max_export_interval,
    cookies,
    csrf_token,
    country,
    store_id,
    output_dir,
):
    base_url, cookie_suffix = get_country_info(country)

    headers = {
        "Content-Type": "application/json",
        "Cookie": cookies,
        "X-CSRF-Token": csrf_token,
    }

    export_ids = OrderedDict()

    interval_start_date = start_date
    while interval_start_date <= end_date:
        interval_end_date = min(
            end_date,
            interval_start_date + datetime.timedelta(days=max_export_interval - 1),
        )
        export_id = generate_reports(interval_start_date, interval_end_date, store_id, base_url, headers)

        export_ids[export_id] = f"{interval_start_date}-{interval_end_date}"

        interval_start_date = interval_end_date + datetime.timedelta(days=1)

        time.sleep(10)  # pause to avoid hitting API rate limit

    output_dir.mkdir(parents=True, exist_ok=True)

    timed_out_exports = set()

    # poll for completion of export
    for export_id, date_range in export_ids.items():
        time.sleep(20)  # start loop with timeout to avoid rate limit
        time_elapsed = 0
        while time_elapsed <= 1200:
            exported_reports = check_report_status(
                store_id, base_url, headers, export_id
            )

            if len(exported_reports) > 0:
                download_reports(
                    exported_reports,
                    date_range,
                    base_url,
                    store_id,
                    headers,
                    output_dir,
                )
                break

            time_elapsed += 20
            if time_elapsed >= 1200:  # large reports can take a long time to generate
                print(
                    f"Status check for request id {export_id} timed out",
                    file=sys.stderr,
                )
                timed_out_exports.add(export_id)
                break

    print("Timed out exports:")
    for timed_out in timed_out_exports:
        print(f"{timed_out} for {export_ids[timed_out]}")


if __name__ == "__main__":
    fetch_reports()
