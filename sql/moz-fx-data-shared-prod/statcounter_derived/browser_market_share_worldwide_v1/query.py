"""Statcounter browser market share worldwide ingestion entrypoint."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import make_app  # noqa: E402

GEOGRAPHIES = [
    ("Worldwide", "worldwide", "ww"),
]

app = make_app(
    geographies=GEOGRAPHIES,
    gcs_blob_prefix="statcounter_data/browser_market_share_worldwide",
    bq_table="browser_market_share_worldwide_v1",
    clustering_fields=["device", "browser"],
)


if __name__ == "__main__":
    app()
