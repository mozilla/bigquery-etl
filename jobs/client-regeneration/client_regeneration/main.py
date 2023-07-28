from typing import List

import click

from google.cloud import bigquery
from datetime import datetime, timedelta, date

DEFAULT_LOOKBACK = 7

DEFAULT_START_DATE = "2022-04-01"
DEFAULT_END_DATE = "2022-06-01"

COLUMN_LIST = [
    "country",
    "device_model",
    "device_manufacturer",
]


def init_replacement_table(client, seed):
    # init the table that will contain the mappings of client_ids to be replaced to their selected replacements
    q = f"""CREATE OR REPLACE TABLE mozdata.analysis.regen_sim_client_replacements_{str(seed)} (
        client_id STRING, -- client_id to be replaced - the "fake new" client that first showed up on `regen_date`.
        label STRING, -- what we used to do the match.
        regen_date DATE, -- the date the "fake new" client id shows up - will be replaced on this day forward.
        regened_last_date DATE, -- the last date we observed the fake new client/the last day it needs to be replaced.
        replacement_id STRING, -- the client_id we sampled from the churn pool to replace `client_id` above.
        last_reported_date DATE, -- this is the day the replacement_id churned in the original history.
        first_seen_date DATE -- this is the first_seen_date of the replacement_id churned in its original history.
    )
    PARTITION BY regen_date
    """

    job = client.query(q)
    job.result()


def init_regen_pool(client, seed, start_date):
    # init a new version of the regen pool, write table to BQ and name it with the random seed that will be used in
    # the sampling
    # TODO: Can we get rid of this DECLARE?
    q = f"""DECLARE start_date DATE DEFAULT DATE("{start_date}"); CREATE OR REPLACE TABLE
    mozdata.analysis.regen_sim_regen_pool_{seed} PARTITION BY regen_date AS SELECT * FROM
    mozdata.analysis.regen_sim_regen_pool_v2 WHERE regen_date >= start_date;"""

    job = client.query(q)
    job.result()


def init_churn_pool(client, seed, start_date, lookback):
    # init a new version of the churn pool, write table to BQ and name it with the random seed that will be used in
    # the sampling
    q = f"""CREATE OR REPLACE TABLE mozdata.analysis.regen_sim_churn_pool_{seed} PARTITION BY last_reported_date AS
    SELECT * FROM mozdata.analysis.regen_sim_churn_pool_v2 WHERE last_reported_date >= DATE_SUB(DATE("{start_date}"),
    INTERVAL {lookback + 1}  DAY);"""

    job = client.query(q)
    job.result()


def sample_for_replacement_bq(client, date, column_list, seed, lookback):
    q = f"""
    -- this is much faster than the pandas way now
    INSERT INTO mozdata.analysis.regen_sim_client_replacements_{str(seed)}
    WITH
      churned AS (
        SELECT
          *,
          CONCAT({", '_', ".join(column_list)}) AS bucket,
        FROM
            mozdata.analysis.regen_sim_churn_pool_{str(seed)}
        WHERE
            last_reported_date BETWEEN DATE_SUB(DATE("{date}"), INTERVAL ({lookback}) DAY) AND DATE("{date}")
      ),

      churned_numbered AS (
        SELECT
          *,
          -- number the clients randomly within bucket. adding the seed makes the sort order reproducable.
          ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY FARM_FINGERPRINT(CONCAT(client_id, {str(seed)}))) AS rn,
        FROM churned
        -- do not sample from from churned clients with FSD equal to the current date that is undergoing replacement (i.e. clients that churn the same day they are new)
        -- these clients will be eligible for replacement, but they won't serve as replacements on the same day.
        WHERE first_seen_date != DATE("{date}")
      ),

      regen AS (
        SELECT
          *,
          CONCAT({", '_', ".join(column_list)}) AS bucket,
        FROM mozdata.analysis.regen_sim_regen_pool_{str(seed)} WHERE regen_date = DATE("{date}")
      ),

      regen_numbered AS (
        SELECT
          *,
          -- this will always sort the clients that will be replaced in the same order. we randomize the order of the
          -- churned clients (see above)
          ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY client_id ) AS rn,
        FROM regen
      )

    SELECT
      r.client_id,
      r.bucket AS label,
      r.regen_date,
      r.regened_last_date,
      c.client_id AS replacement_id,
      c.last_reported_date,
      c.first_seen_date
    FROM regen_numbered r
    LEFT JOIN churned_numbered c
    USING(bucket, rn);
  """

    job = client.query(q)
    job.result()


def update_churn_pool(client, seed, date):
    q = f"""
    -- get the replacements for the given day
    -- WITH replacements AS (
    --   SELECT *
    -- FROM
    --  `mozdata.analysis.regen_sim_client_replacements_{str(seed)}`
    -- WHERE
    --  regen_date = "{str(date)}"
    -- )

    -- remove the clients used as replacements from the churn pool (we only want them to serve as one client's
    -- replacement)
    DELETE FROM `mozdata.analysis.regen_sim_churn_pool_{str(seed)}`
    WHERE client_id IN (
        SELECT replacement_id
        FROM ( SELECT replacement_id
          FROM `mozdata.analysis.regen_sim_client_replacements_{str(seed)}`
          WHERE regen_date = "{str(date)}"
          AND replacement_id IS NOT NULL )
    );

    -- find cases where the regenerated IDs are also in the churn pool - replace them with their sampled replacement
    -- client.
    UPDATE `mozdata.analysis.regen_sim_churn_pool_{str(seed)}` c
    SET client_id = r.replacement_id,
        first_seen_date = r.first_seen_date
    FROM
      (SELECT client_id, replacement_id, first_seen_date
      FROM `mozdata.analysis.regen_sim_client_replacements_{str(seed)}`
      WHERE regen_date = "{str(date)}"
      AND replacement_id IS NOT NULL ) r
    WHERE c.client_id = r.client_id;
    """

    # Here's a potential replacement of the above with DML -> DDL:
    # WITH replacements AS (
    #   SELECT
    #     replacement_id,
    #     client_id
    #   FROM
    #     `mozdata.analysis.regen_sim_client_replacements_{str(seed)}`
    #   WHERE
    #     regen_date = "{str(date)}"
    # ),
    # churn_pool_replacements_removed AS (
    #   SELECT
    #     churn.*
    #   FROM
    #     `mozdata.analysis.regen_sim_churn_pool_{str(seed)}` churn
    #     LEFT JOIN replacements r ON (client_id = replacement_id)
    #   WHERE
    #     replacement_id IS NULL
    # )
    # SELECT
    #   churn.* EXCEPT (client_id),
    #   COALESCE(replacement_id, client_id) as client_id
    # FROM
    #   churn_pool_replacements_removed churn
    #   LEFT JOIN replacements USING(client_id)

    job = client.query(q)
    job.result()


# def write_attributed_clients_history(client, seed, start_date):
#   table_name = """mozdata.analysis.regen_sim_replaced_attributable_clients_v2_{}""".format(str(seed))
#   q = f"""
#   CREATE OR REPLACE TABLE {table_name}
#   AS
#   WITH base AS (
#       SELECT
#         COALESCE(r.replacement_id, c.client_id) AS client_id,
#         COALESCE(r.first_seen_date, c.cohort_date) AS first_seen_date,
#         c.client_id AS original_client_id,
#         r.label,
#         c.submission_date,
#         ad_clicks,
#         searches,
#         searches_with_ads,
#         first_reported_country,
#         sample_id,
#         regened_last_date
#       FROM `mozdata.fenix.attributable_clients_v2` c
#       LEFT JOIN `mozdata.analysis.regen_sim_client_replacements_{str(seed)}` r
#       -- we want the history starting on the regen date.
#       ON (c.client_id = r.client_id) AND (c.submission_date BETWEEN r.regen_date AND r.regened_last_date)
#       AND c.submission_date >= DATE("{str(start_date)}")
#   ),
#
#   numbered AS (
#       SELECT
#           *,
#           ROW_NUMBER() OVER (PARTITION BY client_id, submission_date ORDER BY regened_last_date DESC) AS rn
#       FROM base
#       -- this is to handle the case where the same ID ends a replacement and starts another replacement on the same day, leading to more than one row / client.
#       -- in that case, we ignore the last day of the earlier replacement.
#   )
#
#   SELECT
#       * EXCEPT(regened_last_date, rn)
#   FROM numbered
#   WHERE rn = 1
#   """
#
#   job = client.query(q)
#   job.result()
#   return(table_name)


def write_baseline_clients_daily(client, seed, start_date, end_date):
    table_name = (
        """mozdata.analysis.regen_sim_replaced_baseline_clients_daily_{}""".format(
            str(seed)
        )
    )
    q = f"""
  CREATE OR REPLACE TABLE {table_name}
  AS
  WITH base AS (
      SELECT
        COALESCE(r.replacement_id, c.client_id) AS client_id,
        COALESCE(r.first_seen_date, c.first_seen_date) AS first_seen_date,
        c.client_id AS original_client_id,
        c.first_seen_date AS original_first_seen_date,
        c.submission_date,
        c.country,
        c.device_model,
        udf.safe_sample_id(COALESCE(r.replacement_id, c.client_id)) AS sample_id,
        regened_last_date,
      FROM `mozdata.fenix.baseline_clients_daily` c
      LEFT JOIN `mozdata.analysis.regen_sim_client_replacements_{str(seed)}` r
      -- we want the history starting on the regen date.
      ON (c.client_id = r.client_id) AND (c.submission_date BETWEEN r.regen_date AND r.regened_last_date)
      WHERE c.submission_date BETWEEN DATE("{str(start_date)}") AND DATE("{str(end_date)}")
  ),

  numbered AS (
      SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY client_id, submission_date ORDER BY regened_last_date DESC) AS rn
      FROM base
      -- this is to handle the case where the same ID ends a replacement and starts another replacement on the same day, leading to more than one row / client.
      -- in that case, we ignore the entry from the earlier replacement.
  )

  SELECT
      * EXCEPT(regened_last_date, rn)
  FROM numbered
  WHERE rn = 1
  """
    job = client.query(q)
    job.result()
    return table_name


def write_baseline_clients_daily_with_searches(client, seed, start_date, end_date):
    table_name = """mozdata.analysis.regen_sim_replaced_baseline_clients_daily_with_searches_{}""".format(
        str(seed)
    )
    q = f"""
  CREATE OR REPLACE TABLE {table_name}
  AS
  WITH
  base AS (
      SELECT
        c.client_id,
        c.first_seen_date AS first_seen_date,
        c.submission_date,
        c.country,
        c.device_model,
        COALESCE(a.searches, 0) AS searches,
        COALESCE(a.searches_with_ads, 0) AS searches_with_ads,
        COALESCE(a.ad_clicks, 0) AS ad_clicks,
      FROM `mozdata.fenix.baseline_clients_daily` c
      LEFT JOIN `mozdata.fenix.attributable_clients_v2` a
      USING(client_id, submission_date)
      WHERE c.submission_date BETWEEN DATE("{str(start_date)}") AND DATE("{str(end_date)}")
  ),

  replaced AS (
      SELECT
        COALESCE(r.replacement_id, c.client_id) AS client_id,
        COALESCE(r.first_seen_date, c.first_seen_date) AS first_seen_date,
        udf.safe_sample_id(COALESCE(r.replacement_id, c.client_id)) AS sample_id,
        c.country,
        c.device_model,
        c.submission_date,
        ARRAY_AGG(c.client_id IGNORE NULLS ORDER BY regened_last_date) AS original_client_id,
        ARRAY_AGG(c.first_seen_date IGNORE NULLS ORDER BY regened_last_date) AS original_first_seen_date,
        SUM(searches) AS searches,
        SUM(searches_with_ads) AS searches_with_ads,
        SUM(ad_clicks) AS ad_clicks,
      FROM base c
      LEFT JOIN `mozdata.analysis.regen_sim_client_replacements_{str(seed)}` r
      -- we want the history starting on the regen date.
      ON (c.client_id = r.client_id) AND (c.submission_date BETWEEN r.regen_date AND r.regened_last_date)
      GROUP BY 1,2,3,4,5,6
  )

  SELECT
      *
  FROM replaced
  """

    job = client.query(q)
    job.result()
    return table_name


def init_baseline_clients_yearly(client, seed):
    clients_yearly_name = f"mozdata.analysis.regen_sim_replaced_clients_yearly_{seed}"
    clients_daily_name = f"mozdata.analysis.regen_sim_replaced_baseline_clients_daily_with_searches_{seed}"

    # client_id STRING,
    # first_seen_date DATE,
    # original_client_id STRING,
    # original_first_seen_date DATE,
    # submission_date DATE,
    # country STRING,
    # device_model STRING,
    # sample_id INTEGER,
    # days_seen_bytes BYTES,
    # searches INTEGER,
    # searches_with_ads INTEGER,
    # ad_clicks INTEGER,

    create_yearly_stmt = f"""CREATE OR REPLACE TABLE {clients_yearly_name} 
PARTITION BY
  submission_date
CLUSTER BY
  sample_id,
  client_id
AS
SELECT
  *,
  CAST(NULL AS BYTES) AS days_seen_bytes,
FROM
  {clients_daily_name}
WHERE
  -- Output empty table and read no input rows
  FALSE
    """

    client.query(create_yearly_stmt).result()
    print(f"{clients_yearly_name} created")


def _write_baseline_clients_yearly_partition(
    client, clients_daily, clients_yearly, submission_date
):
    query_stmt = f"""
WITH _current AS (
  SELECT
    * EXCEPT (submission_date),
    udf.bool_to_365_bits(TRUE) AS days_seen_bytes,
  FROM
    `{clients_daily}`
  WHERE
    submission_date = '{submission_date}'
),
_previous AS (
  SELECT
    * EXCEPT (submission_date),
  FROM
    `{clients_yearly}`
  WHERE
    submission_date = DATE_SUB('{submission_date}', INTERVAL 1 DAY)
    AND BIT_COUNT(udf.shift_365_bits_one_day(days_seen_bytes)) > 0
)
SELECT
  DATE('{submission_date}') AS submission_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    udf.combine_adjacent_days_365_bits(
      _previous.days_seen_bytes,
      _current.days_seen_bytes
    ) AS days_seen_bytes
  )
FROM
  _current
FULL OUTER JOIN
  _previous
USING
  (sample_id, client_id)
"""
    partition = submission_date.strftime("%Y%m%d")
    destination_table = f"{clients_yearly}${partition}"
    print(f"Backfilling `{destination_table}` ...")

    query_config = bigquery.QueryJobConfig(
        destination=destination_table,
        default_dataset=f"mozdata.analysis",
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    client.query(query_stmt, job_config=query_config).result()


def write_baseline_clients_yearly(client, seed, start_date, end_date):
    clients_daily_name = f"mozdata.analysis.regen_sim_replaced_baseline_clients_daily_with_searches_{seed}"
    clients_yearly_name = f"mozdata.analysis.regen_sim_replaced_clients_yearly_{seed}"

    dates = [
        date.fromisoformat(start_date) + timedelta(i)
        for i in range(
            (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
        )
    ]

    for submission_date in dates:
        _write_baseline_clients_yearly_partition(
            client, clients_daily_name, clients_yearly_name, submission_date
        )


def write_usage_history(client, seed, start_date, end_date):
    table_name = """mozdata.analysis.regen_sim_replaced_clients_last_seen_{}""".format(
        str(seed)
    )
    q = f"""
  DECLARE start_date DATE DEFAULT "{str(start_date)}";
  DECLARE end_date DATE DEFAULT "{str(end_date)}";

  CREATE TEMP FUNCTION process_bits(bits BYTES) AS (
    STRUCT(
      bits,

      -- An INT64 version of the bits, compatible with bits28 functions
      CAST(CONCAT('0x', TO_HEX(RIGHT(bits, 4))) AS INT64) << 36 >> 36 AS bits28,

      -- An INT64 version of the bits with 64 days of history
      CAST(CONCAT('0x', TO_HEX(RIGHT(bits, 4))) AS INT64) AS bits64,

      -- A field like days_since_seen from clients_last_seen.
      udf.bits_to_days_since_seen(bits) AS days_since_active,

      -- Days since first active, analogous to first_seen_date in clients_first_seen
      udf.bits_to_days_since_first_seen(bits) AS days_since_first_active
    )
  );

  CREATE OR REPLACE TABLE {table_name}
  PARTITION BY submission_date
  AS
  WITH
  alltime AS (
    SELECT
      client_id,
      first_seen_date,
      -- BEGIN
      -- Here we produce bit pattern fields based on the daily aggregates from the
      -- previous step;
      udf.bits_from_offsets(
        ARRAY_AGG(
          DATE_DIFF(end_date, submission_date, DAY)
        )
      ) AS days_active_bits,
      -- END
    FROM
      mozdata.analysis.regen_sim_replaced_baseline_clients_daily_{str(seed)}
    WHERE submission_date BETWEEN start_date AND end_date
    GROUP BY
      client_id, first_seen_date
  )
  SELECT
    end_date - i AS submission_date,
    first_seen_date,
    client_id,
    process_bits(days_active_bits >> i) AS days_active
  FROM
    alltime
  -- The cross join parses each input row into one row per day since the client
  -- was first seen, emulating the format of the existing clients_last_seen table.
  CROSS JOIN
    UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS i
  WHERE
    (days_active_bits >> i) IS NOT NULL
  """

    job = client.query(q)
    job.result()
    return table_name


def create_replacements(
    client: bigquery.Client,
    seed: int,
    start_date: str,
    end_date: str,
    column_list: list,
    lookback: int = 7,
    use_existing: bool = False,
):
    # create a table mapping regenerated clients to their matching replacements.
    # TODO: Can we get rid of these?
    print(f"Creating replacements for seed {seed} from {start_date} to {end_date}")
    replacement_table_name = (
        """mozdata.analysis.regen_sim_client_replacements_{}""".format(str(seed))
    )
    churn_table_name = """mozdata.analysis.regen_sim_churn_pool_{}""".format(str(seed))

    if not use_existing:
        init_replacement_table(client, seed)
        init_regen_pool(client, seed, start_date)
        init_churn_pool(client, seed, start_date, lookback)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    one_day = timedelta(days=1)

    current_dt = start_dt
    while current_dt <= end_dt:
        # get the replacements
        print("""replacing on date {}""".format(str(current_dt)))
        replacements = (
            sample_for_replacement_bq(  # TODO: Is this supposed to return something?
                client, str(current_dt), column_list, seed, lookback
            )
        )
        print("updating churn pool")
        update_churn_pool(client, seed, current_dt)
        current_dt += one_day


def run_simulation(
    client: bigquery.Client,
    seed: int,
    start_date: str,
    column_list: list,
    end_date: str,
    lookback: int,
    actions: List[str],
):
    # at a high level there are two main steps here 1. go day by day and match regenerated client_ids to replacement
    # client_ids that "look like" they churned in the prior `lookback` days. write the matches to a table 2. using
    # the matches from 2, write alternative client histories where regenerated clients are given their replacement ids.

    if "replacement" in actions:
        create_replacements(
            client,
            seed=seed,
            start_date=start_date,
            end_date=end_date,
            column_list=column_list,
            lookback=lookback,
        )

    if "usage-history" in actions:
        write_usage_history(client, seed=seed, start_date=start_date, end_date=end_date)

    if "clients-daily" in actions:
        write_baseline_clients_daily(client, seed=seed, start_date=start_date, end_date=end_date)

    if "clients-daily-with-search" in actions:
        write_baseline_clients_daily_with_searches(client, seed=seed, start_date=start_date, end_date=end_date)

    if "clients-yearly" in actions:
        init_baseline_clients_yearly(client, seed=seed)
        write_baseline_clients_yearly(
            client, seed=seed, start_date=start_date, end_date=end_date
        )

    # if "attributed-clients" in actions:
    # write_attributed_clients_history(client, seed=seed, start_date=start_date)


@click.command()
@click.option("--seed", required=True, type=int, help="Random seed for sampling.")
@click.option(
    "--start_date",
    type=click.DateTime(),
    help="Date to start looking for replacements and writing history.",
    default=DEFAULT_START_DATE,
)
@click.option(
    "--end_date",
    type=click.DateTime(),
    help="Date to stop looking for replacements and writing history.",
    default=DEFAULT_END_DATE,
)
@click.option(
    "--lookback",
    type=int,
    help="How many days to look back for churned clients.",
    default=DEFAULT_LOOKBACK,
)
@click.option(
    "--actions",
    required=True,
    type=click.Choice(
        [
            "replacement",
            "usage-history",
            "clients-daily",
            "clients-daily-with-search",
            "clients-yearly",
            "attributed-clients",
        ],
    ),
    multiple=True,
)
# TODO: column list as a parameter?
def main(seed, start_date, end_date, lookback, actions):
    start_date, end_date = str(start_date.date()), str(end_date.date())

    client = bigquery.Client(project="mozdata")

    run_simulation(
        client,
        seed=seed,
        start_date=start_date,
        column_list=COLUMN_LIST,
        end_date=end_date,
        lookback=lookback,
        actions=actions,
    )


if __name__ == "__main__":
    main()
