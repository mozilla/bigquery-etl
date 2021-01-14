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
        dst_table="kubernetes_not_coerced_to_int_v1",
        metric="custom.googleapis.com/opencensus/not_coerced_to_int",
        execution_time=execution_time,
        interval_hours=interval_hours,
        time_offset=time_offset,
        aggregator=monitoring.Aggregation.Reducer.REDUCE_NONE,
        aligner=monitoring.Aggregation.Aligner.ALIGN_RATE,
        overwrite=False,
        alignment_period=300,
    )


if __name__ == "__main__":
    main()
