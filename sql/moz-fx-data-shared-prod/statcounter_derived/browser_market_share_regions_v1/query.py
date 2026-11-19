"""Statcounter browser market share by region ingestion entrypoint."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import make_app  # noqa: E402

GEOGRAPHIES = [
    ("Africa", "africa", "af"),
    ("Asia", "asia", "as"),
    ("Europe", "europe", "eu"),
    ("North America", "north-america", "na"),
    ("Oceania", "oceania", "oc"),
    ("South America", "south-america", "sa"),
]

app = make_app(
    geographies=GEOGRAPHIES,
    gcs_blob_prefix="statcounter_data/browser_market_share_regions",
    bq_table="browser_market_share_regions_v1",
    clustering_fields=["geography", "device", "browser"],
)


if __name__ == "__main__":
    app()
