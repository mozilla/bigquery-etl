"""bigquery-etl CLI alchemer command."""
from datetime import date, datetime, timedelta

import rich_click as click

from bigquery_etl.alchemer.survey import get_survey_data, insert_to_bq


@click.group(
    help="""Commands for importing alchemer data.

    Examples:

    \b
    # Import data from alchemer (surveygizmo) surveys into BigQuery.
    # The date range is inclusive of the start and end values.
    $ ./bqetl alchemer backfill --start-date=2021-01-01 \\
        --end-date=2021-02-01 \\
        --survey_id=xxxxxxxxxxx \\
        --api_token=xxxxxxxxxxxxxx \\
        --api_secret=xxxxxxxxxxxxxxx \\
        --destination_table=moz-fx-data-shared-prod.telemetry_derived.survey_gizmo_daily_attitudes
    """
)
def alchemer():
    """Create the CLI group for the alchemer command."""
    pass


@alchemer.command()
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option(
    "--end-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today())
)
@click.option("--survey_id", required=True)
@click.option("--api_token", required=True)
@click.option("--api_secret", required=True)
@click.option("--destination_table", required=True)
def backfill(start_date, end_date, survey_id, api_token, api_secret, destination_table):
    """Import data from alchemer (surveygizmo) surveys into BigQuery.

    The date range is inclusive of the start and end values.
    """
    print(
        f"Runing backfill of {survey_id} from {start_date} to {end_date}"
        " into {destination_table}"
    )
    days = (end_date - start_date).days + 1
    start = datetime.utcnow()
    for i in range(days):
        current_date = (start_date + timedelta(i)).isoformat()[:10]
        print(f"Running for {current_date}")
        survey_data = get_survey_data(survey_id, current_date, api_token, api_secret)
        if not survey_data:
            print("No data, skipping insertion...")
            continue
        insert_to_bq(survey_data, destination_table, current_date)
    print(
        f"Processed {days} days in {int((datetime.utcnow()-start).total_seconds())} seconds"
    )
