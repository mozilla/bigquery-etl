"""Supporting script to initialize clients_first_seen_v2."""

# This script runs the query for each `sample_id` in parallel and inserts the results into the destination table.
# By default, it loads data into the analysis dataset. Setting the parameters allows for running into a different one.
# Elapsed time: Each sample_id runs in avg. 35 min. and processes 900 GB of data when running 10 sample_ids in parallel.

# Usage:
# python3 sql/moz-fx-data-shared-prod/analysis/clients_first_seen_v2_full/initiliaze.py --dataset=analysis --table=clients_first_seen_v2


from argparse import ArgumentParser
from functools import partial
from multiprocessing.pool import ThreadPool
from timeit import default_timer

from google.cloud import bigquery

PARTITION_QUERY = """
  INSERT
    `{project_id}.{dataset}.{table}`
    WITH new_profile_ping AS (
      SELECT
        client_id AS client_id,
        sample_id AS sample_id,
        MIN(submission_timestamp) AS submission_timestamp,
        ANY_VALUE(submission_timestamp HAVING MIN submission_timestamp) AS first_seen_date,
        ARRAY_AGG(submission_timestamp IGNORE NULLS ORDER BY submission_timestamp)[
          SAFE_OFFSET(1)
        ] AS second_seen_date,
        ANY_VALUE(application.architecture HAVING MIN submission_timestamp) AS architecture,
        ANY_VALUE(application.build_id HAVING MIN submission_timestamp) AS app_build_id,
        ANY_VALUE(normalized_app_name HAVING MIN submission_timestamp) AS app_name,
        ANY_VALUE(application.platform_version HAVING MIN submission_timestamp) AS platform_version,
        ANY_VALUE(application.vendor HAVING MIN submission_timestamp) AS vendor,
        ANY_VALUE(application.version HAVING MIN submission_timestamp) AS app_version,
        ANY_VALUE(application.xpcom_abi HAVING MIN submission_timestamp) AS xpcom_abi,
        ANY_VALUE(document_id HAVING MIN submission_timestamp) AS document_id,
        ANY_VALUE(
          environment.partner.distribution_id
          HAVING
            MIN submission_timestamp
        ) AS distribution_id,
        ANY_VALUE(
          environment.partner.distribution_version
          HAVING
            MIN submission_timestamp
        ) AS partner_distribution_version,
        ANY_VALUE(
          environment.partner.distributor
          HAVING
            MIN submission_timestamp
        ) AS partner_distributor,
        ANY_VALUE(
          environment.partner.distributor_channel
          HAVING
            MIN submission_timestamp
        ) AS partner_distributor_channel,
        ANY_VALUE(environment.partner.partner_id HAVING MIN submission_timestamp) AS partner_id,
        ANY_VALUE(
          environment.settings.attribution.campaign
          HAVING
            MIN submission_timestamp
        ) AS attribution_campaign,
        ANY_VALUE(
          environment.settings.attribution.content
          HAVING
            MIN submission_timestamp
        ) AS attribution_content,
        ANY_VALUE(
          environment.settings.attribution.experiment
          HAVING
            MIN submission_timestamp
        ) AS attribution_experiment,
        ANY_VALUE(
          environment.settings.attribution.medium
          HAVING
            MIN submission_timestamp
        ) AS attribution_medium,
        ANY_VALUE(
          environment.settings.attribution.source
          HAVING
            MIN submission_timestamp
        ) AS attribution_source,
        ANY_VALUE(
          environment.settings.default_search_engine_data.load_path
          HAVING
            MIN submission_timestamp
        ) AS engine_data_load_path,
        ANY_VALUE(
          environment.settings.default_search_engine_data.name
          HAVING
            MIN submission_timestamp
        ) AS engine_data_name,
        ANY_VALUE(
          environment.settings.default_search_engine_data.origin
          HAVING
            MIN submission_timestamp
        ) AS engine_data_origin,
        ANY_VALUE(
          environment.settings.default_search_engine_data.submission_url
          HAVING
            MIN submission_timestamp
        ) AS engine_data_submission_url,
        ANY_VALUE(environment.system.apple_model_id HAVING MIN submission_timestamp) AS apple_model_id,
        ANY_VALUE(metadata.geo.city HAVING MIN submission_timestamp) AS city,
        ANY_VALUE(metadata.geo.db_version HAVING MIN submission_timestamp) AS db_version,
        ANY_VALUE(metadata.geo.subdivision1 HAVING MIN submission_timestamp) AS subdivision1,
        ANY_VALUE(normalized_channel HAVING MIN submission_timestamp) AS normalized_channel,
        ANY_VALUE(normalized_country_code HAVING MIN submission_timestamp) AS country,
        ANY_VALUE(normalized_os HAVING MIN submission_timestamp) AS normalized_os,
        ANY_VALUE(normalized_os_version HAVING MIN submission_timestamp) AS normalized_os_version,
        ANY_VALUE(
          payload.processes.parent.scalars.startup_profile_selection_reason
          HAVING
            MIN submission_timestamp
        ) AS startup_profile_selection_reason,
        ANY_VALUE(
          environment.settings.attribution.dltoken
          HAVING
            MIN submission_timestamp
        ) AS download_token,
        ANY_VALUE(
          environment.settings.attribution.dlsource
          HAVING
            MIN submission_timestamp
        ) AS download_source
      FROM
        `moz-fx-data-shared-prod.telemetry.new_profile`
      WHERE
          DATE(submission_timestamp) >= '2017-06-26'
          AND sample_id = {sample_id}
      GROUP BY
        client_id,
        sample_id
    ),
    shutdown_ping AS (
      SELECT
        client_id AS client_id,
        sample_id AS sample_id,
        MIN(submission_timestamp) AS submission_timestamp,
        ANY_VALUE(submission_timestamp HAVING MIN submission_timestamp) AS first_seen_date,
        ARRAY_AGG(submission_timestamp IGNORE NULLS ORDER BY submission_timestamp)[
          SAFE_OFFSET(1)
        ] AS second_seen_date,
        ANY_VALUE(application.architecture HAVING MIN submission_timestamp) AS architecture,
        ANY_VALUE(application.build_id HAVING MIN submission_timestamp) AS app_build_id,
        ANY_VALUE(normalized_app_name HAVING MIN submission_timestamp) AS app_name,
        ANY_VALUE(application.platform_version HAVING MIN submission_timestamp) AS platform_version,
        ANY_VALUE(application.vendor HAVING MIN submission_timestamp) AS vendor,
        ANY_VALUE(application.version HAVING MIN submission_timestamp) AS app_version,
        ANY_VALUE(application.xpcom_abi HAVING MIN submission_timestamp) AS xpcom_abi,
        ANY_VALUE(document_id HAVING MIN submission_timestamp) AS document_id,
        ANY_VALUE(
          environment.partner.distribution_id
          HAVING
            MIN submission_timestamp
        ) AS distribution_id,
        ANY_VALUE(
          environment.partner.distribution_version
          HAVING
            MIN submission_timestamp
        ) AS partner_distribution_version,
        ANY_VALUE(
          environment.partner.distributor
          HAVING
            MIN submission_timestamp
        ) AS partner_distributor,
        ANY_VALUE(
          environment.partner.distributor_channel
          HAVING
            MIN submission_timestamp
        ) AS partner_distributor_channel,
        ANY_VALUE(environment.partner.partner_id HAVING MIN submission_timestamp) AS partner_id,
        ANY_VALUE(
          environment.settings.attribution.campaign
          HAVING
            MIN submission_timestamp
        ) AS attribution_campaign,
        ANY_VALUE(
          environment.settings.attribution.content
          HAVING
            MIN submission_timestamp
        ) AS attribution_content,
        ANY_VALUE(
          environment.settings.attribution.experiment
          HAVING
            MIN submission_timestamp
        ) AS attribution_experiment,
        ANY_VALUE(
          environment.settings.attribution.medium
          HAVING
            MIN submission_timestamp
        ) AS attribution_medium,
        ANY_VALUE(
          environment.settings.attribution.source
          HAVING
            MIN submission_timestamp
        ) AS attribution_source,
        ANY_VALUE(
          environment.settings.default_search_engine_data.load_path
          HAVING
            MIN submission_timestamp
        ) AS engine_data_load_path,
        ANY_VALUE(
          environment.settings.default_search_engine_data.name
          HAVING
            MIN submission_timestamp
        ) AS engine_data_name,
        ANY_VALUE(
          environment.settings.default_search_engine_data.origin
          HAVING
            MIN submission_timestamp
        ) AS engine_data_origin,
        ANY_VALUE(
          environment.settings.default_search_engine_data.submission_url
          HAVING
            MIN submission_timestamp
        ) AS engine_data_submission_url,
        ANY_VALUE(environment.system.apple_model_id HAVING MIN submission_timestamp) AS apple_model_id,
        ANY_VALUE(metadata.geo.city HAVING MIN submission_timestamp) AS city,
        ANY_VALUE(metadata.geo.db_version HAVING MIN submission_timestamp) AS db_version,
        ANY_VALUE(metadata.geo.subdivision1 HAVING MIN submission_timestamp) AS subdivision1,
        ANY_VALUE(normalized_channel HAVING MIN submission_timestamp) AS normalized_channel,
        ANY_VALUE(normalized_country_code HAVING MIN submission_timestamp) AS country,
        ANY_VALUE(normalized_os HAVING MIN submission_timestamp) AS normalized_os,
        ANY_VALUE(normalized_os_version HAVING MIN submission_timestamp) AS normalized_os_version,
        ANY_VALUE(
          payload.processes.parent.scalars.startup_profile_selection_reason
          HAVING
            MIN submission_timestamp
        ) AS startup_profile_selection_reason,
        ANY_VALUE(
          environment.settings.attribution.dltoken
          HAVING
            MIN submission_timestamp
        ) AS download_token,
        ANY_VALUE(
          environment.settings.attribution.dlsource
          HAVING
            MIN submission_timestamp
        ) AS download_source
      FROM
        `moz-fx-data-shared-prod.telemetry.first_shutdown`
      WHERE
          DATE(submission_timestamp) >= '2018-10-30'
          AND sample_id = {sample_id}
      GROUP BY
        client_id,
        sample_id
    ),
    main_ping AS (
      SELECT
        client_id AS client_id,
        sample_id AS sample_id,
        TIMESTAMP(MIN(submission_date)) AS submission_timestamp,
        TIMESTAMP(ANY_VALUE(submission_date HAVING MIN submission_date)) AS first_seen_date,
        TIMESTAMP(
          ARRAY_AGG(submission_date IGNORE NULLS ORDER BY submission_date)[SAFE_OFFSET(1)]
        ) AS second_seen_date,
        CAST(NULL AS STRING) AS architecture,
        ANY_VALUE(app_build_id HAVING MIN submission_date) AS app_build_id,
        ANY_VALUE(app_name HAVING MIN submission_date) AS app_name,
        CAST(NULL AS STRING) AS platform_version,
        ANY_VALUE(vendor HAVING MIN submission_date) AS vendor,
        ANY_VALUE(app_version HAVING MIN submission_date) AS app_version,
        CAST(NULL AS STRING) AS xpcom_abi,
        CAST(NULL AS STRING) AS document_id,
        ANY_VALUE(distribution_id HAVING MIN submission_date) AS distribution_id,
        CAST(NULL AS STRING) AS partner_distribution_version,
        CAST(NULL AS STRING) AS partner_distributor,
        CAST(NULL AS STRING) AS partner_distributor_channel,
        CAST(NULL AS STRING) AS partner_id,
        ANY_VALUE(attribution.campaign HAVING MIN submission_date) AS attribution_campaign,
        ANY_VALUE(attribution.content HAVING MIN submission_date) AS attribution_content,
        ANY_VALUE(attribution.experiment HAVING MIN submission_date) AS attribution_experiment,
        ANY_VALUE(attribution.medium HAVING MIN submission_date) AS attribution_medium,
        ANY_VALUE(attribution.source HAVING MIN submission_date) AS attribution_source,
        ANY_VALUE(
          default_search_engine_data_load_path
          HAVING
            MIN submission_date
        ) AS engine_data_load_path,
        ANY_VALUE(
          default_search_engine_data_name
          HAVING
            MIN submission_date
        ) AS engine_data_name,
        ANY_VALUE(
          default_search_engine_data_origin
          HAVING
            MIN submission_date
        ) AS engine_data_origin,
        ANY_VALUE(
          default_search_engine_data_submission_url
          HAVING
            MIN submission_date
        ) AS engine_data_submission_url,
        CAST(NULL AS STRING) AS apple_model_id,
        ANY_VALUE(city HAVING MIN submission_date) AS city,
        CAST(NULL AS STRING) AS db_version,
        ANY_VALUE(geo_subdivision1 HAVING MIN submission_date) AS subdivision1,
        ANY_VALUE(normalized_channel HAVING MIN submission_date) AS normalized_channel,
        ANY_VALUE(country HAVING MIN submission_date) AS country,
        ANY_VALUE(os HAVING MIN submission_date) AS normalized_os,
        ANY_VALUE(normalized_os_version HAVING MIN submission_date) AS normalized_os_version,
        CAST(NULL AS STRING) AS startup_profile_selection_reason,
        ANY_VALUE(attribution.dltoken HAVING MIN submission_date) AS download_token,
        CAST(NULL AS STRING) AS download_token
      FROM
        `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
      WHERE
          submission_date >= '2016-03-12'
          AND sample_id = {sample_id}
      GROUP BY
        client_id,
        sample_id
    ),
    unioned AS (
      SELECT
        *,
        'new_profile' AS ping_source
      FROM
        new_profile_ping
      UNION ALL
      SELECT
        *,
        'shutdown' AS ping_source
      FROM
        shutdown_ping
      UNION ALL
      SELECT
        *,
        'main' AS ping_source
      FROM
        main_ping
    ),
    unioned_with_all_dates AS (
      SELECT
        client_id,
        ARRAY_CONCAT(
          ARRAY_AGG(
            STRUCT(
              CAST(DATE(unioned.first_seen_date) AS STRING) AS value,
              ping_source AS value_source,
              DATETIME(unioned.first_seen_date) AS value_date
            ) IGNORE NULLS
          ),
          ARRAY_AGG(
            STRUCT(
              CAST(DATE(unioned.second_seen_date) AS STRING) AS value,
              ping_source AS value_source,
              DATETIME(unioned.second_seen_date) AS value_date
            ) IGNORE NULLS
          )
        ) AS seen_dates
      FROM
        unioned
      GROUP BY
        client_id
    ),
    unioned_second_dates AS (
      SELECT
        client_id,
        DATE(
          ARRAY_AGG(DISTINCT _date.value IGNORE NULLS ORDER BY _date.value ASC)[SAFE_OFFSET(1)]
        ) AS second_seen_date,
      FROM
        unioned_with_all_dates
      LEFT JOIN
        UNNEST(seen_dates) AS _date
      WHERE
        _date.value_date IS NOT NULL
      GROUP BY
        1
    ),
    _current AS (
      SELECT
        client_id,
        CAST(
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(STRUCT(CAST(sample_id AS STRING), ping_source, DATETIME(submission_timestamp)))
          ).earliest_value AS INT
        ) AS sample_id,
        DATE(
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(
              STRUCT(CAST(submission_timestamp AS STRING), ping_source, DATETIME(submission_timestamp))
            )
          ).earliest_date
        ) AS first_seen_date,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(architecture AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS architecture,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(app_build_id AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS app_build_id,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(app_name AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS app_name,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(platform_version AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS platform_version,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(vendor AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS vendor,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(app_version AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS app_version,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(xpcom_abi AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS xpcom_abi,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(document_id AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS document_id,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(distribution_id AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS distribution_id,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(
              CAST(partner_distribution_version AS STRING),
              ping_source,
              DATETIME(submission_timestamp)
            )
          )
        ).earliest_value AS partner_distribution_version,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(partner_distributor AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS partner_distributor,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(
              CAST(partner_distributor_channel AS STRING),
              ping_source,
              DATETIME(submission_timestamp)
            )
          )
        ).earliest_value AS partner_distributor_channel,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(partner_id AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS partner_id,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(engine_data_load_path AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS engine_data_load_path,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(engine_data_name AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS engine_data_name,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(engine_data_origin AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS engine_data_origin,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(
              CAST(engine_data_submission_url AS STRING),
              ping_source,
              DATETIME(submission_timestamp)
            )
          )
        ).earliest_value AS engine_data_submission_url,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(apple_model_id AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS apple_model_id,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(city AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS city,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(subdivision1 AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS subdivision1,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(normalized_channel AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS normalized_channel,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(country AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS country,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(normalized_os AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS normalized_os,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(normalized_os_version AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS normalized_os_version,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(
              CAST(startup_profile_selection_reason AS STRING),
              ping_source,
              DATETIME(submission_timestamp)
            )
          )
        ).earliest_value AS startup_profile_selection_reason,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(STRUCT(CAST(download_token AS STRING), ping_source, DATETIME(submission_timestamp)))
        ).earliest_value AS download_token,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(download_source AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS download_source,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(attribution_campaign AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS attribution_campaign,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(attribution_content AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS attribution_content,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(attribution_experiment AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS attribution_experiment,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(attribution_medium AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS attribution_medium,
        mozfun.norm.get_earliest_value(
          ARRAY_AGG(
            STRUCT(CAST(attribution_source AS STRING), ping_source, DATETIME(submission_timestamp))
          )
        ).earliest_value AS attribution_source,
        STRUCT(
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(
              STRUCT(CAST(submission_timestamp AS STRING), ping_source, DATETIME(submission_timestamp))
            )
          ).earliest_value_source AS first_seen_date__source_ping,
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(
              STRUCT(CAST(attribution_campaign AS STRING), ping_source, DATETIME(submission_timestamp))
            )
          ).earliest_value_source AS attribution_campaign__source_ping,
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(
              STRUCT(CAST(attribution_content AS STRING), ping_source, DATETIME(submission_timestamp))
            )
          ).earliest_value_source AS attribution_content__source_ping,
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(
              STRUCT(
                CAST(attribution_experiment AS STRING),
                ping_source,
                DATETIME(submission_timestamp)
              )
            )
          ).earliest_value_source AS attribution_experiment__source_ping,
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(
              STRUCT(CAST(attribution_medium AS STRING), ping_source, DATETIME(submission_timestamp))
            )
          ).earliest_value_source AS attribution_medium__source_ping,
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(
              STRUCT(CAST(attribution_source AS STRING), ping_source, DATETIME(submission_timestamp))
            )
          ).earliest_value_source AS attribution_source__source_ping,
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(
              STRUCT(CAST(download_token AS STRING), ping_source, DATETIME(submission_timestamp))
            )
          ).earliest_value_source AS download_token__source_ping,
          mozfun.norm.get_earliest_value(
            ARRAY_AGG(
              STRUCT(CAST(download_source AS STRING), ping_source, DATETIME(submission_timestamp))
            )
          ).earliest_value_source AS download_source__source_ping,
          'main' IN UNNEST(ARRAY_AGG(ping_source)) AS reported_main_ping,
          'new_profile' IN UNNEST(ARRAY_AGG(ping_source)) AS reported_new_profile_ping,
          'shutdown' IN UNNEST(ARRAY_AGG(ping_source)) AS reported_shutdown_ping
        ) AS metadata
      FROM
        unioned
      WHERE
        client_id IS NOT NULL
      GROUP BY
        client_id
    ),
    _current_with_second_date AS (
      SELECT
        _current.client_id,
        _current.sample_id,
        _current.first_seen_date,
        unioned_second_dates.second_seen_date AS second_seen_date,
        _current.* EXCEPT (client_id, sample_id, first_seen_date)
      FROM
        _current
      LEFT JOIN
        unioned_second_dates
      USING
        (client_id)
    ),
    _previous AS (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod.analysis.clients_first_seen_v2_full`
    )
    SELECT
      IF(_previous.client_id IS NULL, _current, _previous).* REPLACE (
        COALESCE(_previous.second_seen_date, _current.second_seen_date) AS second_seen_date
      )
    FROM
      _previous
    FULL JOIN
      _current_with_second_date _current
    USING
      (client_id)
    ;
"""

parser = ArgumentParser()
parser.add_argument(
    "--project_id",
    "--project-id",
    default="moz-fx-data-shared-prod",
    help="ID of the project in which to find tables",
)
parser.add_argument(
    "--dataset",
    default="analysis",
    help="Dataset name to create the table clients_first_seen_v2.",
)
parser.add_argument(
    "--table",
    default="clients_first_seen_v2",
    help="Name of the destination table to be created.",
)
parser.add_argument(
    "--parallelism",
    "-p",
    default=10,
    help="Number of threads run in parallel.",
)
parser.add_argument("--dry_run", default=False, help="Issue a dry run.")


def _insert_data_for_sample_id(
    client, job_config, project_id, dataset, table, sample_id
):
    """Insert data for one sample_id into dataset.table."""
    print(f"Insert data into table {dataset}.{table} for sample_id={sample_id}...")
    client.query(
        PARTITION_QUERY.format(
            sample_id=sample_id, project_id=project_id, dataset=dataset, table=table
        ),
        job_config=job_config,
    ).result()


def main():
    """Initialize clients_first_seen_v2 by inserting data per sample_id in parallel."""
    args = parser.parse_args()
    client = bigquery.Client(args.project_id)

    if args.dry_run:
        print("Do a dry run.")
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    else:
        job_config = bigquery.QueryJobConfig(dry_run=False, use_query_cache=False)

    with ThreadPool(args.parallelism) as pool:
        start = default_timer()
        # Process all sample_ids in parallel.
        pool.map(
            partial(
                _insert_data_for_sample_id,
                client,
                job_config,
                args.project_id,
                args.dataset,
                args.table,
            ),
            list(range(0, 10)),
        )
        print(f"Job completed in {default_timer()-start}")


if __name__ == "__main__":
    main()
