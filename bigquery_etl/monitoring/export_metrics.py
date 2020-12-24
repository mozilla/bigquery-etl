import datetime
import os
import time
from typing import Dict, List

from google.cloud import bigquery, monitoring
import pandas as pd


def get_time_series(
    client: monitoring.MetricServiceClient,
    target_project: str,
    filter_string: str,
    aggregation: monitoring.Aggregation,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
) -> List[Dict]:
    interval = monitoring.TimeInterval(
        {
            "start_time": {"seconds": int(start_time.timestamp())},
            "end_time": {"seconds": int(end_time.timestamp())},
        }
    )

    results = client.list_time_series(
        monitoring.ListTimeSeriesRequest(
            {
                "name": f"projects/{target_project}",
                "filter": filter_string,
                "interval": interval,
                "view": monitoring.ListTimeSeriesRequest.TimeSeriesView.FULL,
                "aggregation": aggregation,
            }
        )
    )

    data_points = []

    for result in results:
        data_points.extend(
            [
                {
                    "value": point.value.double_value,
                    "timestamp": point.interval.start_time,
                    **result.resource.labels,
                    **result.metric.labels,
                }
                for point in result.points
            ]
        )

    return sorted(data_points, key=lambda x: x["timestamp"])


def write_to_bq(
    client: bigquery.Client, target_dataset, table_name, data_points, overwrite
) -> bigquery.LoadJob:
    """Convert list of dict to dataframe and load into Bigquery."""
    load_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        if overwrite
        else bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema_update_options=None
        if overwrite
        else bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        time_partitioning=bigquery.TimePartitioning(field="timestamp"),
        autodetect=True,
    )

    df = pd.DataFrame(data=data_points)
    target_table = bigquery.table.TableReference(target_dataset, table_name)
    load_response = client.load_table_from_dataframe(
        df, target_table, job_config=load_config, parquet_compression="UNCOMPRESSED"
    )
    return load_response


# TODO: support additional metric filters and aggregation parameters
def export_metrics(
    monitoring_project,
    dst_project,
    dst_dataset,
    dst_table,
    metric,
    execution_time,
    time_offset=0,
    interval_hours=1,
    overwrite=False,
    aggregator=monitoring.Aggregation.Reducer.REDUCE_NONE,
    aligner=monitoring.Aggregation.Aligner.ALIGN_MEAN,
    alignment_period=300,
):
    """
    Fetch given metric from cloud monitoring and write results to designated table.

    :param monitoring_project: Project containing the monitoring workspace
    :param dst_project: Project to write results to
    :param dst_dataset: Bigquery dataset to write results to
    :param dst_table: Bigquery table to write results to
    :param metric: Metric and resource identifier (e.g. pubsub.googleapis.com/topic/send_request_count)
    :param execution_time: End of the time interval to export
    :param time_offset: Number of hours to offset interval to account for delayed metrics
    :param interval_hours: Number of hours to export, ending at execution time
    :param overwrite: Overwrite destination table (default is append)
    :param aggregator: Cloud monitoring series aggregator
    :param aligner: Cloud monitoring series aligner
    :param alignment_period: Seconds between each point in the time series
    """
    client = monitoring.MetricServiceClient()

    bq_client = bigquery.Client(dst_project)
    target_dataset = bigquery.DatasetReference(dst_project, dst_dataset)

    # Force timezone to be UTC so intervals match exported metrics
    os.environ["TZ"] = "UTC"
    time.tzset()

    # TODO: MAKE IDEMPOTENT

    # Loading large dataframes into bigquery may cause a resources exceeded exception
    interval_width = 24
    for interval_offset in range(0, interval_hours, interval_width):
        offset = time_offset + interval_offset
        end_time = execution_time - datetime.timedelta(hours=offset)
        start_time = execution_time - datetime.timedelta(
            hours=min(offset + interval_width, time_offset + interval_hours)
        )
        print(f"Fetching values for {start_time} to {end_time}")

        metric_filter = f'metric.type = "{metric}"'

        aggregation = monitoring.Aggregation(
            {
                "alignment_period": {"seconds": alignment_period},
                "cross_series_reducer": aggregator,
                "per_series_aligner": aligner,
            }
        )

        time_series_data = get_time_series(
            client, monitoring_project, metric_filter, aggregation, start_time, end_time
        )

        if len(time_series_data) == 0:
            print(f"No data points found for interval {start_time} to {end_time}")
            continue

        print(
            f"Retrieved {len(time_series_data)} data points for"
            f" {start_time} to {end_time}"
        )

        response = write_to_bq(
            bq_client,
            target_dataset,
            dst_table,
            time_series_data,
            overwrite=overwrite and interval_offset == 0,
        )
        response.result()

        print(f"Wrote to {dst_project}.{dst_dataset}.{dst_table}")
