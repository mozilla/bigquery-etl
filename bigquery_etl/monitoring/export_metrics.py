import datetime
import os
import time
from typing import Dict, List

from google.cloud import bigquery, monitoring
from google.cloud.exceptions import NotFound


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
                    # stringify datetime so it's serializable
                    "timestamp": str(point.interval.start_time),
                    **result.resource.labels,
                    **result.metric.labels,
                }
                for point in result.points
            ]
        )

    return sorted(data_points, key=lambda x: x["timestamp"])


def write_to_bq(
    client: bigquery.Client,
    target_table: bigquery.TableReference,
    data_points: List[Dict],
    overwrite: bool,
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
    load_response = client.load_table_from_json(
        data_points,
        target_table,
        job_config=load_config,
    )
    return load_response


def get_max_timestamp(
    client: bigquery.Client,
    target_table: bigquery.TableReference,
) -> datetime.datetime:
    """Get latest timestamp in the target table."""
    try:
        results = client.query(
            f"""
            SELECT MAX(timestamp) AS timestamp 
            FROM {target_table.dataset_id}.{target_table.table_id}
            """
        ).result()
    except NotFound:
        return None
    rows = [row for row in results]
    if len(rows) == 0:
        return None
    return rows[0].timestamp


# TODO: support additional metric filters and aggregation parameters
def export_metrics(
    monitoring_project: str,
    dst_project: str,
    dst_dataset: str,
    dst_table: str,
    metric: str,
    execution_time: datetime.datetime,
    time_offset: int = 0,
    interval_hours: int = 1,
    overwrite: bool = False,
    aggregator: monitoring.Aggregation.Reducer = monitoring.Aggregation.Reducer.REDUCE_NONE,
    aligner: monitoring.Aggregation.Aligner = monitoring.Aggregation.Aligner.ALIGN_MEAN,
    alignment_period: int = 300,
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
    monitoring_client = monitoring.MetricServiceClient()

    bq_client = bigquery.Client(dst_project)

    # Force timezone to be UTC so intervals match exported metrics
    os.environ["TZ"] = "UTC"
    time.tzset()

    # TODO: MAKE IDEMPOTENT

    # Larger intervals cause run time to increase superlinearly and takes a lot of memory
    interval_width = 200
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
            monitoring_client,
            monitoring_project,
            metric_filter,
            aggregation,
            start_time,
            end_time,
        )

        print(
            f"Retrieved {len(time_series_data)} data points for"
            f" {start_time} to {end_time}"
        )

        target_dataset = bigquery.DatasetReference(dst_project, dst_dataset)
        target_table = bigquery.table.TableReference(target_dataset, dst_table)

        # get latest timestamp in destination table to avoid overlap
        if not overwrite and len(time_series_data) > 0:
            max_timestamp = get_max_timestamp(bq_client, target_table)
            if max_timestamp is not None:
                time_series_data = list(
                    filter(
                        lambda x: datetime.datetime.fromisoformat(x["timestamp"])
                        > max_timestamp,
                        time_series_data,
                    )
                )

        if len(time_series_data) == 0:
            print(f"No new data points found for interval {start_time} to {end_time}")
            continue

        response = write_to_bq(
            bq_client,
            target_table,
            time_series_data,
            overwrite=overwrite and interval_offset == 0,
        )
        response.result()

        print(f"Wrote to {dst_project}.{dst_dataset}.{dst_table}")
