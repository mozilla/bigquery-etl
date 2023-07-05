import click

from google.cloud import bigquery
from datetime import datetime, timedelta

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
    SET client_id = r.replacement_id
    FROM
      (SELECT client_id, replacement_id
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
#     table_name = (
#         """mozdata.analysis.regen_sim_replaced_attributable_clients_{}""".format(
#             str(seed)
#         )
#     )
#     q = f"""
#     -- this query replaces regenerated (i.e. 'fake new') client_ids with their matches in `attributable_clients`
#     -- the main purpose here is to get the daily ad click and search data.
#
#     CREATE OR REPLACE TABLE {table_name}
#     AS
#         SELECT
#           COALESCE(r.replacement_id, c.client_id) AS client_id,
#           COALESCE(r.first_seen_date, c.cohort_date) AS first_seen_date,
#           c.client_id AS original_client_id,
#           r.label,
#           c.submission_date,
#           ad_clicks,
#           searches,
#           searches_with_ads,
#           country,
#           sample_id
#         FROM `mozdata.fenix.attributable_clients` c --switch to v2 when ready
#         LEFT JOIN `mozdata.analysis.regen_sim_client_replacements_{str(seed)}` r
#         -- we want the history starting on the regen date.
#         ON (c.client_id = r.client_id) AND (c.submission_date BETWEEN r.regen_date AND r.regened_last_date)
#         AND c.submission_date >= DATE("{start_date}")
#     """
#
#     job = client.query(q)
#     job.result()
#
#     return table_name
#
#
# def write_usage_history(client, seed, start_date):
#     table_name = """mozdata.analysis.regen_sim_replaced_clients_last_seen_{}""".format(
#         str(seed)
#     )
#     q = f"""
#     -- this query replaces regenerated (i.e. 'fake new') client_ids with their matches in `clients_last_seen`
#     -- the main purpose here is to get their usage history for the markov states
#     -- one complication here is that we need 'stitch' the bit pattern histories of the churned and replaced clients
#     -- together using bitwise or
#
#     CREATE OR REPLACE TABLE {table_name}
#     AS
#     WITH
#       replacement_history AS (
#         SELECT
#           r.replacement_id AS client_id,
#           c.submission_date,
#           days_seen_bits AS pre_churn_days_seen_bits
#         FROM `mozdata.analysis.regen_sim_client_replacements_{str(seed)}` r
#         -- or clients last seen joined??
#         LEFT JOIN `mozdata.fenix.baseline_clients_last_seen` c
#         -- we want the history starting on the regen date.
#         ON (r.replacement_id = c.client_id) AND (c.submission_date BETWEEN r.regen_date AND DATE_ADD(r.regen_date, INTERVAL 27 DAY))
#         WHERE c.submission_date >= DATE("{start_date}")
#       ),
#
#       -- get the replaced client's history starting on the regen (replacement) date
#       -- this is their first day - prior days will be 0s. we want to fill those in with any active days from the replacement client
#       replaced_history AS (
#         SELECT
#           r.client_id AS original_client_id,
#           r.replacement_id,
#           c.submission_date,
#           days_seen_bits AS post_churn_days_seen_bits
#         FROM `mozdata.analysis.regen_sim_client_replacements_{str(seed)}` r
#         LEFT JOIN `mozdata.fenix.baseline_clients_last_seen` c
#         ON (r.client_id = c.client_id) AND (c.submission_date BETWEEN r.regen_date AND DATE_ADD(r.regen_date, INTERVAL 27 DAY))
#         WHERE c.submission_date >= DATE("{start_date}")
#       ),
#
#       -- combine the histories
#       combined_history AS (
#         SELECT
#             d.original_client_id,
#             d.replacement_id,
#             r.submission_date,
#             IF(pre_churn_days_seen_bits IS NOT NULL, (pre_churn_days_seen_bits | post_churn_days_seen_bits), post_churn_days_seen_bits) AS days_seen_bits
#         FROM replacement_history r
#         INNER JOIN replaced_history d
#         ON r.client_id = d.replacement_id AND r.submission_date = d.submission_date
#       )
#
#       -- now we need to get the complete cls history for each match, using the above to "fix" the histories during the transition period
#
#         SELECT
#           -- use the replacement_id if its there,
#           COALESCE(r.replacement_id, l.client_id) AS client_id,
#           l.client_id AS original_client_id,
#           COALESCE(r.first_seen_date, l.first_seen_date) AS first_seen_date,
#           r.label,
#           l.submission_date,
#           -- prefer the fixed history if its there
#           COALESCE(c.days_seen_bits, l.days_seen_bits) AS days_seen_bits,
#         -- left join cls to the replacements first to slot in the new id throughout the history
#         FROM mozdata.fenix.baseline_clients_last_seen l
#         LEFT JOIN `mozdata.analysis.regen_sim_client_replacements_{str(seed)}` r
#         ON (l.client_id = r.client_id) AND (l.submission_date BETWEEN r.regen_date AND r.regened_last_date)
#         -- now join onto the combined histories for the 27 days following the replacement date
#         LEFT JOIN combined_history c
#         ON (l.client_id = c.original_client_id) AND (l.submission_date = c.submission_date)
#         WHERE l.submission_date >= DATE("{start_date}")
#     """
#     job = client.query(q)
#     job.result()
#     return table_name
#


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
):
    # at a high level there are two main steps here 1. go day by day and match regenerated client_ids to replacement
    # client_ids that "look like" they churned in the prior `lookback` days. write the matches to a table 2. using
    # the matches from 2, write alternative client histories where regenerated clients are given their replacement ids.

    create_replacements(
        client,
        seed=seed,
        start_date=start_date,
        end_date=end_date,
        column_list=column_list,
        lookback=lookback,
    )
    # TODO:
    # write_attributed_clients_history(client, seed=seed, start_date=start_date)
    # write_usage_history(client, seed=seed, start_date=start_date)


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
# TODO: column list as a parameter?
def main(seed, start_date, end_date, lookback):
    start_date, end_date = str(start_date.date()), str(end_date.date())
    print(seed, start_date, end_date, lookback)

    client = bigquery.Client(project="mozdata")
    run_simulation(
        client,
        seed=seed,
        start_date=start_date,
        column_list=COLUMN_LIST,
        end_date=end_date,
        lookback=lookback,
    )


if __name__ == "__main__":
    main()
