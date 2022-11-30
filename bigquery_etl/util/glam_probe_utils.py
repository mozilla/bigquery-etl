"""Functions that retrieve allow or block lists of probes."""
from datetime import date

from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

six_months = date.today() + relativedelta(months=+6)

GLAM_USAGE_SERVICE = "https://glam.telemetry.mozilla.org/api/v1/usage/"
RECENT_PROBE_DAYS = 90


def query_hotlist():
    """Query glam_hotlist to use."""
    project = "moz-fx-data-shared-prod"
    client = bigquery.Client(project)
    job = client.query(
        """
            SELECT
            metrics
            FROM
                `dev_telemetry_derived.glam_hotlist` h
            INNER JOIN (
            SELECT
                MAX(date) AS max_date
            FROM
                `dev_telemetry_derived.glam_hotlist`) m
            ON
            h.date = m.max_date
        """
    )
    results = job.result()
    hotlist = []
    for row in results:
        hotlist = row.metrics  # Only one row expected according to the query above
    return set(hotlist)


def get_etl_excluded_probes_quickfix(product):
    """Provide a static list of probes that must be excluded from aggregation."""
    # See https://github.com/mozilla/glam/issues/1865
    forbidden_probes_by_product = {
        "fenix": {},
        "desktop": {"sqlite_store_open", "sqlite_store_query"},
    }
    return forbidden_probes_by_product[product]


def probe_is_recent_legacy(probe_data):
    """Check probe data and returns if probe was created in the last RECENT_PROBE_DAYS."""
    # TODO: Temporarily return false in favor of hardcoded list of recently used probes
    return False
    """
    probe_created_on = datetime.strptime(
        probe_data["first_added"]["nightly"], "%Y-%m-%d %H:%M:%S"
    )
    return (datetime.now() - probe_created_on).days <= RECENT_PROBE_DAYS
    """


def probe_is_recent_glean(probe, product):
    """Return whether probe was created in the last 90 days."""
    # TODO: Temporarily return false in favor of hardcoded list of recently used probes
    return False
    """
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
    """
