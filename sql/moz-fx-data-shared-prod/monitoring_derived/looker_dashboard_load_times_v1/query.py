import os

import click
import looker_sdk

from looker_sdk import methods40 as methods
from looker_sdk import models40 as models
from looker_sdk import error
import json
from google.cloud import bigquery


def setup_sdk(client_id, client_secret, instance) -> methods.Looker40SDK:
    os.environ["LOOKERSDK_BASE_URL"] = instance
    os.environ["LOOKERSDK_API_VERSION"] = "4.0"
    os.environ["LOOKERSDK_VERIFY_SSL"] = "true"
    os.environ["LOOKERSDK_TIMEOUT"] = "9000"
    os.environ["LOOKERSDK_CLIENT_ID"] = client_id
    os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret

    return looker_sdk.init40()


def run_query(sdk, date):
    """Runs the specified query with the specified filters."""
    lookml_date = f"date({date.year}, {date.month}, {date.day})"

    request = models.WriteQuery(
        model="system__activity",
        view="dashboard_performance",
        fields=[
            "dashboard_history_stats.total_runtime",
            "dashboard_page_event_stats.seconds_until_controller_initialized",
            "dashboard_page_event_stats.seconds_until_dom_content_loaded",
            "dashboard_page_event_stats.seconds_until_metadata_loaded",
            "dashboard_performance.seconds_until_dashboard_run_start",
            "dashboard_performance.seconds_until_first_data_received",
            "dashboard_performance.seconds_until_first_tile_finished_rendering",
            "dashboard_performance.seconds_until_last_data_received",
            "dashboard_performance.seconds_until_last_tile_finished_rendering",
            "dashboard_performance.dash_id",
            "dashboard_performance.dashboard_page_session",
            "dashboard_performance.first_event_at_date",
        ],
        pivots=None,
        fill_fields=None,
        filters={"dashboard.is_legacy": "No", "dashboard.moved_to_trash": "No"},
        sorts=["dashboard_history_stats.total_runtime"],
        limit=5000,
        column_limit=50,
        total=None,
        row_total=None,
        subtotals=None,
        dynamic_fields=None,
        query_timezone=None,
        filter_expression="${dashboard_performance.first_event_at_date} = "
        + lookml_date,
    )
    try:
        json_ = sdk.run_inline_query("json", request, cache=False)
    except error.SDKError:
        raise Exception("Error running query")
    else:
        json_resp = json.loads(json_)
    return json_resp


def import_performance_data_for_date(sdk, destination_table, date):
    result = run_query(sdk, date)
    # field identifiers are formatted like dashboard_performance.<name>, only the latter part is relevant
    result = [{key.split(".")[1]: value for key, value in r.items()} for r in result]
    result = [
        {
            "submission_date" if key == "first_event_at_date" else key: value
            for key, value in r.items()
        }
        for r in result
    ]

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.schema = (
        bigquery.SchemaField("total_runtime", "FLOAT64"),
        bigquery.SchemaField("seconds_until_controller_initialized", "FLOAT64"),
        bigquery.SchemaField("seconds_until_dom_content_loaded", "FLOAT64"),
        bigquery.SchemaField("seconds_until_metadata_loaded", "FLOAT64"),
        bigquery.SchemaField("seconds_until_dashboard_run_start", "FLOAT64"),
        bigquery.SchemaField("seconds_until_first_data_received", "FLOAT64"),
        bigquery.SchemaField("seconds_until_first_tile_finished_rendering", "FLOAT64"),
        bigquery.SchemaField("seconds_until_last_data_received", "FLOAT64"),
        bigquery.SchemaField("seconds_until_last_tile_finished_rendering", "FLOAT64"),
        bigquery.SchemaField("dash_id", "STRING"),
        bigquery.SchemaField("dashboard_page_session", "STRING"),
        bigquery.SchemaField("submission_date", "DATE"),
    )
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="submission_date",
    )

    partition_date = date.strftime("%Y%m%d")
    client.load_table_from_json(
        result,
        f"{destination_table}${partition_date}",
        job_config=job_config,
    ).result()


@click.command()
@click.option("--client_id", envvar="LOOKER_API_CLIENT_ID", required=True)
@click.option("--client_secret", envvar="LOOKER_API_CLIENT_SECRET", required=True)
@click.option("--instance_uri", default="https://mozilla.cloud.looker.com", required=True)
@click.option(
    "--destination_table",
    "--destination-table",
    required=True,
    default="moz-fx-data-shared-prod.monitoring_derived.looker_dashboard_load_times_v1",
)
@click.option("--date", required=True, type=click.DateTime(formats=["%Y-%m-%d"]))
def main(
    client_id: str, client_secret: str, instance_uri: str, destination_table, date
):
    sdk = setup_sdk(client_id, client_secret, instance_uri)

    click.echo(f"Importing Looker dashboard performance data for {date}")
    import_performance_data_for_date(sdk, destination_table, date)


if __name__ == "__main__":
    main()
