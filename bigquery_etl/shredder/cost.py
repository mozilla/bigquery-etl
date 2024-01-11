#!/usr/bin/env python

"""Report estimated cost to run shredder."""

import warnings
from argparse import ArgumentParser
from datetime import datetime, timedelta
from math import ceil
from textwrap import dedent

from google.cloud import bigquery

from ..util import standard_args
from ..util.bigquery_id import sql_table_id
from .config import DELETE_TARGETS

JOBS_QUERY = """
SELECT
  SUM(total_bytes_processed) AS total_bytes_processed,
  SUM(total_slot_ms) AS slot_millis,
  destination_table.project_id AS project,
  destination_table.dataset_id,
  destination_table.table_id
FROM
  `moz-fx-data-bq-batch-prod.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE
  job_id IN (
    -- most recent unqualified job id for each task
    SELECT
      SPLIT(ARRAY_AGG(job_id ORDER BY job_created DESC)[OFFSET(0)], ".")[OFFSET(1)]
    FROM
      `moz-fx-data-shredder.shredder_state.shredder_state`
    GROUP BY
      task_id
  )
  AND state = "DONE"
  AND error_result IS NULL
  AND destination_table.dataset_id = "telemetry_derived"
GROUP BY
  destination_table.project_id,
  destination_table.dataset_id,
  destination_table.table_id
"""

TABLES_QUERY = (
    "WITH tables AS ("
    + "\n  UNION ALL".join(
        f"\n  SELECT * FROM `{project}.{dataset_id}.__TABLES__`"
        for project, dataset_id in {
            (target.project, target.dataset_id) for target in DELETE_TARGETS
        }
    )
    + """
)
SELECT
  project_id AS project,
  dataset_id,
  table_id,
  size_bytes AS num_bytes,
FROM
  tables
"""
)

# calculate the minimum bytes_per_second a job needs to process to reduce cost
# by using flat rate pricing instead of on demand pricing at 100% utilization
seconds_per_day = 60 * 60 * 24
seconds_per_year = seconds_per_day * 365
flat_rate_dollars_per_year_per_slot = 12 * 8500 / 500
on_demand_bytes_per_dollar = 2**40 / 5
min_flat_rate_bytes_per_second_per_slot = (
    on_demand_bytes_per_dollar * flat_rate_dollars_per_year_per_slot / seconds_per_year
)

# translate min_flat_rate_bytes_per_second_per_slot to slot_millis_per_byte
slot_millis_per_second_per_slot = 1000
min_flat_rate_slot_millis_per_byte = (
    slot_millis_per_second_per_slot / min_flat_rate_bytes_per_second_per_slot
)

parser = ArgumentParser()
standard_args.add_argument(
    parser,
    "-s",
    "--slots",
    default=1000,
    type=int,
    help="Number of reserved slots currently available",
)
standard_args.add_argument(
    parser,
    "-S",
    "--state-table",
    "--state_table",
    default="moz-fx-data-shredder.shredder_state.shredder_state",
    help="Table used to store shredder state for script/shredder_delete",
)
standard_args.add_argument(
    parser,
    "-d",
    "--days",
    default=28,
    type=int,
    help="Number of days shredder has to run",
)


def mean(iterable):
    """Calculate the mean of an iterable of optional values.

    Each item in iterable may be a value with an implicit weight of 1 or a
    tuple of value and weight. When value is None exclude it from the mean.
    """
    total_value = total_weight = 0
    for item in iterable:
        if isinstance(item, tuple):
            value, weight = item
        else:
            value, weight = item, 1
        if value is None:
            continue
        total_value += value * weight
        total_weight += weight
    if total_weight != 0:
        return total_value / total_weight


def get_bytes_per_second(jobs, flat_rate_slots, table=None, verbose=False):
    """Calculcate the speed of jobs matching table in bytes per second."""
    if table is not None:
        table_id = sql_table_id(table)
        jobs = [job for job in jobs if sql_table_id(job) == table_id]
    if jobs:
        total_bytes_processed, slot_millis = map(
            sum, zip(*((job.total_bytes_processed, job.slot_millis) for job in jobs))
        )
        if total_bytes_processed == 0 or slot_millis == 0:
            return None
        slot_millis_per_byte = slot_millis / total_bytes_processed
        bytes_per_second = total_bytes_processed / (
            slot_millis / slot_millis_per_second_per_slot / flat_rate_slots
        )
        if verbose:
            print(
                "shredder is processing "
                f"{table.table_id + ' at ' if table else ''}"
                f"{bytes_per_second*60/2**30:.3f} GiB/min "
                f"using {flat_rate_slots} slots"
            )
            # report cost vs on-demand
            efficiency = slot_millis_per_byte / min_flat_rate_slot_millis_per_byte
            tense = "cheaper" if efficiency <= 1 else "more expensive"
            efficiency_percent = abs(100 - efficiency * 100)
            print(
                f"processing speed{' for ' + table.table_id if table else ''} is "
                f"{efficiency_percent:.0f}% {tense} than on-demand at 100% utilization"
            )
        return bytes_per_second
    return None


def main():
    """Report estimated cost to run shredder."""
    args = parser.parse_args()
    flat_rate_slots = args.slots
    runs_per_year = 365 / args.days

    # translate days per run to seconds per run
    seconds_per_run = args.days * seconds_per_day

    # determine how fast shredder is running
    warnings.filterwarnings("ignore", module="google.auth._default")
    client = bigquery.Client()
    jobs = list(client.query(JOBS_QUERY).result())

    table_ids = {sql_table_id(target) for target in DELETE_TARGETS}
    tables = [
        table
        for table in client.query(TABLES_QUERY).result()
        if sql_table_id(table) in table_ids
    ]
    flat_rate_tables = [table for table in tables if table.table_id != "main_v4"]
    on_demand_tables = [table for table in tables if table.table_id == "main_v4"]

    # estimate how much it costs to process flat-rate tables
    flat_rate_speeds_and_bytes = [
        (get_bytes_per_second(jobs, flat_rate_slots, table), table.num_bytes)
        for table in flat_rate_tables
    ]
    # mean weighted by table.num_bytes for tables with jobs
    mean_bytes_per_second = mean(flat_rate_speeds_and_bytes)
    # calculate using mean_bytes_per_second for tables with no jobs
    slots_needed = sum(
        num_bytes
        / seconds_per_run
        / (table_bytes_per_second or mean_bytes_per_second)
        * flat_rate_slots
        for table_bytes_per_second, num_bytes in flat_rate_speeds_and_bytes
    )
    # slots are reserved in increments of 500
    slots_reserved = ceil(slots_needed / 500) * 500
    flat_rate_cost = slots_reserved * flat_rate_dollars_per_year_per_slot
    flat_rate_num_bytes = sum(table.num_bytes for table in flat_rate_tables)

    # estimate how much it costs to process on-demand queries
    on_demand_num_bytes = sum(table.num_bytes for table in on_demand_tables)
    on_demand_cost = on_demand_num_bytes * runs_per_year / on_demand_bytes_per_dollar

    query_cost = flat_rate_cost + on_demand_cost

    # estimate how much it increases costs to store data
    total_num_bytes = on_demand_num_bytes + flat_rate_num_bytes
    added_storage_cost_per_gigabyte_per_year = 0.01 * 12  # $0.01 per GiB
    bytes_per_gigabyte = 2**30
    storage_cost = (
        total_num_bytes * added_storage_cost_per_gigabyte_per_year / bytes_per_gigabyte
    )

    # estimate how much cost will grow due to retention policies
    # days since the start of stable tables
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    start_of_stable = datetime(2018, 10, 30).date()
    days_in_tables = (today - start_of_stable).days
    # normalize 25 month retention to 2 years 1 month with 28 day "months"
    days_in_retention = 28 * (13 * 2 + 1)
    # estimate how much data will grow before retention kicks in
    data_growth = 0
    if days_in_tables < days_in_retention:
        data_growth = days_in_retention / days_in_tables - 1
    # translate data_growth to cost
    on_demand_cost_growth = on_demand_cost * data_growth
    storage_cost_growth = storage_cost * data_growth
    total_cost_growth = on_demand_cost_growth + storage_cost_growth
    slots_reserved_growth = (
        ceil(slots_needed * (1 + data_growth) / 500) * 500 - slots_reserved
    )
    flat_rate_growth_msg = (
        "$0 is is flat-rate query cost because existing "
        "rounded up query capacity should suffice."
    )
    if slots_reserved_growth > 0:
        flat_rate_cost_growth = (
            slots_reserved_growth * flat_rate_dollars_per_year_per_slot
        )
        flat_rate_growth_msg = (
            f"${flat_rate_cost_growth:,.0f} is flat-rate query cost for "
            f"{slots_reserved_growth} additional reserved slots"
        )
        total_cost_growth += flat_rate_cost_growth

    total_cost = query_cost + storage_cost + total_cost_growth

    print(
        dedent(
            f"""
            Cost
            ===

            As of {today}, the cost of this project is estimated to be
            ${total_cost:,.0f}/year. Of this, ${query_cost:,.0f} is direct cost of
            scanning and deleting data, ${storage_cost:,.0f} is increased storage cost
            due to active (vs. long-term) storage pricing, and
            ${total_cost_growth:,.0f} is predicted cost increases as more data is
            accumulated and stored.

            On-demand queries
            ---

            telemetry_stable.main_v4 contains {on_demand_num_bytes/2**50:.3f} PiB of
            data, which is scanned by queries using on-demand pricing at $5 per TiB¹
            every {args.days} days, costing ${on_demand_cost:,.0f}/year.

            Flat-rate queries
            ---

            The rest of the tables impacted contain {flat_rate_num_bytes/2**50:.3f} PiB
            of data, which is scanned by queries using flat-rate pricing at $8,500 per
            500 reserved slots¹, and requires approximately {slots_needed:.0f} slots in
            order to complete every {args.days} days, rounded up to {slots_reserved}
            slots, costing ${flat_rate_cost:,.0f}/year.

            Active storage
            ---

            Combined all of the tables contain {total_num_bytes/2**50:.3f} PiB of data,
            which is stored using active storage pricing at $0.02 per GiB per month¹
            and would otherwise be stored using long-term storage pricing at $0.01 per
            GiB per month¹, thus increasing storage cost by ${storage_cost:,.0f}/year.

            Data volume growth
            ---

            The above costs total ${query_cost+storage_cost:,.0f}/year, but that does
            not account for table sizes increasing over time. If all tables had a
            retention policy of 25 months, as of {today} held data from
            {start_of_stable} through {yesterday}, and had a stable daily data volume,
            then total data volume would increase by {100*data_growth:.0f}% before any
            data were dropped due to retention policies.

            Assuming a linear impact on queries, this would increase cost by
            ${total_cost_growth:,.0f}/year. Of this, ${storage_cost_growth:,.0f} is
            storage cost, ${on_demand_cost_growth:,.0f} is on-demand query cost, and
            {flat_rate_growth_msg}.


            ¹ https://cloud.google.com/bigquery/pricing#pricing_summary (this doc
            currently uses the annual $8500 per 500 slots flat-rate query pricing
            rather than the monthly $10000 per 500 slots)
            """
        )
    )


if __name__ == "__main__":
    main()
