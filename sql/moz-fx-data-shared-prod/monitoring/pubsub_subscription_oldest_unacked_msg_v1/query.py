import datetime

import click
from google.cloud import monitoring

from bigquery_etl.monitoring import export_metrics


@click.command()
@click.option("--execution-time", type=datetime.datetime.fromisoformat, required=True)
@click.option("--interval-hours", type=int, required=True)
@click.option("--time-offset", type=int, required=True)
def main(execution_time, interval_hours, time_offset):
    export_metrics.export_metrics(
        monitoring_project="moz-fx-data-ingesti-prod-579d",
        dst_project="moz-fx-data-shared-prod",
        dst_dataset="monitoring",
        dst_table="pubsub_subscription_oldest_unacked_msg_v1",
        metric="pubsub.googleapis.com/subscription/oldest_unacked_message_age",
        execution_time=execution_time,
        interval_hours=interval_hours,
        time_offset=time_offset,
        aggregator=monitoring.Aggregation.Reducer.REDUCE_NONE,
        aligner=monitoring.Aggregation.Aligner.ALIGN_MEAN,
        overwrite=False,
        alignment_period=300,
    )


if __name__ == "__main__":
    main()
