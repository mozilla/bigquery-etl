"""Functions that retrieve allow or block lists of probes."""
from urllib.request import Request, urlopen
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


def temp_hardcoded_most_used_probes():
    """Provide a temporary static list of most used probes - used for testing dev etl."""
    return [
        "pdfjs_editing",
        "wr_rasterize_glyphs_time",
        "wr_renderer_time",
        "gfx_macos_video_low_power",
        "browser_ui_interaction_content_context",
        "composite_time",
        "glean_baseline_duration",
        "cookie_retrieval_samesite_problem",
        "dns_trr_lookup_time3",
        "gc_budget_overrun",
    ]


def get_most_used_probes():
    """Fetch and provide probes that have been used lately in Glam.

    Important: The API does not distinguish from different products (e.g.: fenix, desktop),
    so make sure the list of probes from this function goes through a filter
    to avoid computing probes for a different product.
    """
    # Temporarily returning a list of hardcoded probes, for testing
    return temp_hardcoded_most_used_probes()
    """
    from_date = (date.today() - relativedelta(months=3)).strftime("%Y%m%d")
    url_req = Request(
        f"{GLAM_USAGE_SERVICE}?fromDate={from_date}&fields=probe_name&actionType=PROBE_SEARCH&agg=count",
        None,
        {

        },
    )
    probe_names = []
    with urlopen(url_req) as url:
        data_str = gzip.decompress(url.read()).decode()
        data = json.loads(data_str)
        probe_names = [entry["probe_name"] for entry in data]

    return probe_names
    """


def probe_is_recent_legacy(probe_data):
    """Check probe data and returns if probe was created in the last RECENT_PROBE_DAYS."""
    # Temporarily return false in favor of hardcoded list of recently used probes
    return False
    """
    probe_created_on = datetime.strptime(
        probe_data["first_added"]["nightly"], "%Y-%m-%d %H:%M:%S"
    )
    return (datetime.now() - probe_created_on).days <= RECENT_PROBE_DAYS
    """


def probe_is_recent_glean(probe, product):
    """Return whether probe was created in the last 90 days."""
    product_map = {
        "firefox_desktop": "firefox_desktop",
        "org_mozilla_fenix": "fenix",
    }

    if product not in product_map.keys():
        raise Exception(f"Product not supported: {product}")

    product_path = product_map[product]
    domain = "https://dictionary.telemetry.mozilla.org"
    url = f"{domain}/data/{product_path}/metrics/data_{probe}.json"
    url_req = Request(url)

    with urlopen(url_req) as url:
        data = json.loads(url.read())
        probe_created_on = datetime.strptime(
            data["date_first_seen"], "%Y-%m-%d %H:%M:%S"
        )

    return (datetime.now() - probe_created_on).days <= RECENT_PROBE_DAYS
