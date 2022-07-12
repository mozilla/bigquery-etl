"""Functions that retrieve allow or block lists of probes."""
import urllib.request
import gzip
import json
from datetime import date
from dateutil.relativedelta import relativedelta

six_months = date.today() + relativedelta(months=+6)

GLAM_USAGE_SERVICE = "https://glam.telemetry.mozilla.org/api/v1/usage/"


def get_etl_excluded_probes_quickfix(product):
    """Provide a static list of probes that must be excluded from aggregation.
    """
    # See https://github.com/mozilla/glam/issues/1865
    forbidden_probes_by_product = {
        "fenix": {},
        "desktop": {"sqlite_store_open", "sqlite_store_query"},
    }
    return forbidden_probes_by_product[product]


def get_most_used_probes():
    """Fetch and provide probes that have been used lately in Glam.
    Important: The API does not distinguish between different products (e.g.: fenix, desktop), so make sure the list of probes from this function goes through a filter to avoid computing probes for a different product
    """
    from_date = (date.today() - relativedelta(months=3)).strftime("%Y%m%d")
    url_req = f"{GLAM_USAGE_SERVICE}?fromDate={from_date}&fields=probe_name&actionType=PROBE_SEARCH&agg=count"
    probe_names = []
    with urllib.request.urlopen(url_req) as url:
        data = json.loads(gzip.decompress(url.read()).decode())
        probe_names.append([entry["probe_name"] for entry in data])

    return probe_names
