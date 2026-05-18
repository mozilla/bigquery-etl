import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import typer

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import PipelineConfig, main

REGIONS = [
    ("Africa", "africa", "af"),
    ("Asia", "asia", "as"),
    ("Europe", "europe", "eu"),
    ("North America", "north-america", "na"),
    ("Oceania", "oceania", "oc"),
    ("South America", "south-america", "sa"),
]

BASE_URL = "https://gs.statcounter.com/browser-market-share/{device_slug}/{region_slug}/chart.php?bar=1&device={device_name}&device_hidden={device_slug}&statType_hidden=browser&region_hidden={region_code}&granularity=daily&statType=Browser&region={region_name}&csv=1"

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
    sources = [
        (
            name,
            device,
            BASE_URL.format(
                device_slug=device.lower(),
                region_slug=slug,
                device_name=device,
                region_code=code,
                region_name=quote(name, safe=""),
            ),
        )
        for name, slug, code in REGIONS
        for device in ("Desktop", "Mobile")
    ]
    main(
        PipelineConfig(
            sources=sources,
            gcs_blob_prefix="statcounter_data/browser_market_share_regions",
            bq_table="browser_market_share_regions_v1",
            clustering_fields=["geography", "device", "browser"],
        ),
        date_from=date_from.date() if date_from else None,
        date_to=date_to.date() if date_to else None,
    )


if __name__ == "__main__":
    app()
