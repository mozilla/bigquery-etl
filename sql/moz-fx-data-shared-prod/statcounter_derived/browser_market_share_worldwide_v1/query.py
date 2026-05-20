"""Statcounter browser market share worldwide ingestion entrypoint."""

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import PipelineConfig, Source, main  # noqa: E402

BASE_URL_DESKTOP = (
    "https://gs.statcounter.com/browser-market-share/desktop/worldwide/chart.php"
    "?bar=1&device=Desktop&device_hidden=desktop&statType_hidden=browser"
    "&region_hidden=ww&granularity=daily&statType=Browser&region=Worldwide&csv=1"
)
BASE_URL_MOBILE = (
    "https://gs.statcounter.com/browser-market-share/mobile/worldwide/chart.php"
    "?bar=1&device=Mobile&device_hidden=mobile&statType_hidden=browser"
    "&region_hidden=ww&granularity=daily&statType=Browser&region=Worldwide&csv=1"
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
    main(
        PipelineConfig(
            sources=[
                Source(
                    geography="Worldwide", device="Desktop", base_url=BASE_URL_DESKTOP
                ),
                Source(
                    geography="Worldwide", device="Mobile", base_url=BASE_URL_MOBILE
                ),
            ],
            gcs_blob_prefix="statcounter_data/browser_market_share_worldwide",
            bq_table="browser_market_share_worldwide_v1",
            clustering_fields=["device", "browser"],
        ),
        date_from=date_from.date() if date_from else None,
        date_to=date_to.date() if date_to else None,
    )


if __name__ == "__main__":
    app()
