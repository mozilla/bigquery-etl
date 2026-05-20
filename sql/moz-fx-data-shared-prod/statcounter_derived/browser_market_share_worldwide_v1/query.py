"""Statcounter browser market share worldwide ingestion entrypoint."""

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import typer

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import PipelineConfig, Source, main  # noqa: E402

GEOGRAPHIES = [
    ("Worldwide", "worldwide", "ww"),
]

BASE_URL = (
    "https://gs.statcounter.com/browser-market-share/{device_slug}/{region_slug}"
    "/chart.php?bar=1&device={device_name}&device_hidden={device_slug}"
    "&statType_hidden=browser&region_hidden={region_code}&granularity=daily"
    "&statType=Browser&region={region_name}&csv=1"
)

app = typer.Typer()


@app.command()
def run(
    date_from: Optional[datetime] = typer.Option(
        None,
        formats=["%Y-%m-%d"],
        help="Start date (YYYY-MM-DD). Defaults to yesterday.",
    ),
    date_to: Optional[datetime] = typer.Option(
        None,
        formats=["%Y-%m-%d"],
        help="End date (YYYY-MM-DD). Defaults to --date-from.",
    ),
) -> None:
    """Run the worldwide pipeline for the given date range."""
    sources = [
        Source(
            geography=name,
            device=device,
            base_url=BASE_URL.format(
                device_slug=device.lower(),
                region_slug=slug,
                device_name=device,
                region_code=code,
                region_name=quote(name, safe=""),
            ),
        )
        for name, slug, code in GEOGRAPHIES
        for device in ("Desktop", "Mobile")
    ]
    main(
        PipelineConfig(
            sources=sources,
            gcs_blob_prefix="statcounter_data/browser_market_share_worldwide",
            bq_table="browser_market_share_worldwide_v1",
            clustering_fields=["device", "browser"],
        ),
        date_from=date_from.date() if date_from else None,
        date_to=date_to.date() if date_to else None,
    )


if __name__ == "__main__":
    app()
