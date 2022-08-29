"""Functions that retrieve allow or block lists of probes."""
from urllib.request import Request, urlopen
import gzip
import json
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

six_months = date.today() + relativedelta(months=+6)

GLAM_USAGE_SERVICE = "https://glam.telemetry.mozilla.org/api/v1/usage/"
RECENT_PROBE_DAYS = 90

def get_etl_excluded_probes_quickfix(product):
    """Provide a static list of probes that must be excluded from aggregation."""
    # See https://github.com/mozilla/glam/issues/1865
    forbidden_probes_by_product = {
        "fenix": {},
        "desktop": {"sqlite_store_open", "sqlite_store_query"},
    }
    return forbidden_probes_by_product[product]


def get_most_used_probes():
    """Fetch and provide probes that have been used lately in Glam.
    Important: The API does not distinguish from different products (e.g.: fenix, desktop), so make sure the list of probes from this function goes through a filter to avoid computing probes for a different product
    """
    from_date = (date.today() - relativedelta(months=3)).strftime("%Y%m%d")
    url_req = Request(
        f"{GLAM_USAGE_SERVICE}?fromDate={from_date}&fields=probe_name&actionType=PROBE_SEARCH&agg=count",
        None,
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:105.0) Gecko/20100101 Firefox/105.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://glam.telemetry.mozilla.org/firefox/probe/gc_animation_ms/explore?activeBuckets=%5B%22always%22%2C%22never%22%2C%22sometimes%22%5D&channel=beta&currentPage=1',
            'Content-Type': 'application/json',
            'Origin': 'https://glam.telemetry.mozilla.org',
            'Connection': 'keep-alive',
            'Cookie': '_ga=GA1.2.119231309.1644872089; GCP_IAP_UID=110159661541651238953; _conv_v=vi%3A1*sc%3A2*cs%3A1659550195*fs%3A1651172239*pv%3A3*exp%3A%7B%7D*ps%3A1651172239; _conv_r=s%3Awww.google.com*m%3Aorganic*t%3A*c%3A; _ga_27EDMJ02H6=GS1.1.1659645197.4.0.1659645197.0; _ga_X4N05QV93S=GS1.1.1658521128.13.1.1658521188.0; _ga_MQ7767QQQW=GS1.1.1661457208.3.1.1661458495.0.0.0; _ga_2VC139B3XV=GS1.1.1658517422.3.1.1658517519.0; session=vKPYyAtK_9txqUHIhSlrVQ|1661875023|JRI4Dr8_95uInwFy77-e67zRzbk; _ga_R3H4BDP5J2=GS1.1.1660939631.1.0.1660939631.0.0.0',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'TE': 'trailers'
        },
    )
    probe_names = []
    with urlopen(url_req) as url:
        data_str = gzip.decompress(url.read()).decode()
        data = json.loads(data_str)
        probe_names = [entry["probe_name"] for entry in data]

    return probe_names

def probe_is_recent_legacy(probe_data):
    probe_created_on = datetime.strptime(
        probe_data["first_added"]["nightly"], "%Y-%m-%d %H:%M:%S"
    )
    return (datetime.now() - probe_created_on).days <= RECENT_PROBE_DAYS

def probe_is_recent_glean(probe, product):
    """Return whether probe was created in the last 90 days"""
    product_map = {
        "firefox_desktop": "firefox_desktop",
        "org_mozilla_fenix": "fenix",
    }

    if product not in product_map.keys():
        raise Exception(f"Product not supported: {product}")

    product_path = product_map[product]
    url = f"https://dictionary.telemetry.mozilla.org/data/{product_path}/metrics/data_{probe}.json"
    url_req = Request(url)

    with urlopen(url_req) as url:
        data = json.loads(url.read())
        probe_created_on = datetime.strptime(
            data["date_first_seen"], "%Y-%m-%d %H:%M:%S"
        )

    return (datetime.now() - probe_created_on).days <= RECENT_PROBE_DAYS
