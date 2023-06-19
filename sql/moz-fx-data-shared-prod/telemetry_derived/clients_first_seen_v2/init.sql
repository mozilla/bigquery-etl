--Query to initialize clients_first_seen_v2
-- Earliest ping dates for each ping used in the query.
DECLARE earliest_new_profile_date DATE DEFAULT '2017-06-26';

DECLARE earliest_main_date DATE DEFAULT '2016-03-12';

DECLARE earliest_shutdown_date DATE DEFAULT '2018-10-30';

-- Starting date to query pings.
DECLARE starting_date DATE DEFAULT '2020-01-01';

-- Names of pings to store in the metadata.
DECLARE new_profile_ping_name STRING DEFAULT 'new_profile';

DECLARE shutdown_ping_name STRING DEFAULT 'shutdown';

DECLARE main_ping_name STRING DEFAULT 'main';

WITH new_profile_ping AS (
  SELECT
    client_id AS client_id,
    MIN(sample_id) AS sample_id,
    MIN(submission_timestamp) AS submission_timestamp,
    ARRAY_AGG(application.architecture IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS architecture,
    ARRAY_AGG(application.platform_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS platform_version,
    ARRAY_AGG(application.xpcom_abi IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS xpcom_abi,
    ARRAY_AGG(document_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS document_id,
    ARRAY_AGG(experiments.key IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS experiments_key,
    ARRAY_AGG(experiments.value.branch IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS experiments_branch,
    ARRAY_AGG(experiments.value.enrollment_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS experiments_enrollment_id,
    ARRAY_AGG(experiments.value.type IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS experiments_type,
    ARRAY_AGG(
      environment.partner.distribution_version IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS partner_distribution_version,
    ARRAY_AGG(environment.partner.distributor IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS partner_distributor,
    ARRAY_AGG(
      environment.partner.distributor_channel IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS partner_distributor_channel,
    ARRAY_AGG(environment.partner.partner_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS partner_id,
    ARRAY_AGG(
      environment.settings.attribution.campaign IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_campaign,
    ARRAY_AGG(
      environment.settings.attribution.content IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_content,
    ARRAY_AGG(
      environment.settings.attribution.experiment IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_experiment,
    ARRAY_AGG(
      environment.settings.attribution.medium IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_medium,
    ARRAY_AGG(
      environment.settings.attribution.source IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_source,
    ARRAY_AGG(environment.system.apple_model_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS apple_model_id,
    ARRAY_AGG(metadata.geo.db_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS db_version,
    ARRAY_AGG(
      payload.processes.parent.scalars.startup_profile_selection_reason IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS startup_profile_selection_reason,
    ARRAY_AGG(
      environment.settings.attribution.dltoken IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS download_token,
    ARRAY_AGG(
      environment.settings.attribution.dlsource IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS download_source
  FROM
    telemetry.new_profile
  LEFT JOIN
    UNNEST(environment.experiments) AS experiments
  WHERE
    DATE(submission_timestamp) >= starting_date
  GROUP BY
    client_id
),
shutdown_ping AS (
  SELECT
    client_id AS client_id,
    MIN(submission_timestamp) AS submission_timestamp,
    ARRAY_AGG(
      environment.settings.attribution.campaign IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_campaign,
    ARRAY_AGG(
      environment.settings.attribution.content IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_content,
    ARRAY_AGG(
      environment.settings.attribution.experiment IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_experiment,
    ARRAY_AGG(
      environment.settings.attribution.medium IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_medium,
    ARRAY_AGG(
      environment.settings.attribution.source IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS attribution_source,
    ARRAY_AGG(
      environment.settings.attribution.dltoken IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS download_token,
    ARRAY_AGG(
      environment.settings.attribution.dlsource IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS download_source
  FROM
    telemetry.first_shutdown
  WHERE
    DATE(submission_timestamp) >= starting_date
  GROUP BY
    client_id
),
main_ping AS (
  SELECT
    client_id AS client_id,
    MIN(sample_id) AS sample_id,
    MIN(submission_date) AS submission_date,
    MIN(submission_timestamp_min) AS submission_timestamp,
    ARRAY_AGG(app_build_id IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS app_build_id,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(app_display_version IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS app_display_version,
    ARRAY_AGG(app_name IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS app_name,
    ARRAY_AGG(vendor IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS vendor,
    ARRAY_AGG(app_version IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS app_version,
    ARRAY_AGG(distribution_id IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS distribution_id,
    ARRAY_AGG(attribution.campaign IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_campaign,
    ARRAY_AGG(attribution.content IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_content,
    ARRAY_AGG(attribution.experiment IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_experiment,
    ARRAY_AGG(attribution.medium IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_medium,
    ARRAY_AGG(attribution.source IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_source,
    ARRAY_AGG(default_search_engine_data_load_path IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS engine_data_load_path,
    ARRAY_AGG(default_search_engine_data_name IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS engine_data_name,
    ARRAY_AGG(default_search_engine_data_origin IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS engine_data_origin,
    ARRAY_AGG(default_search_engine_data_submission_url IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS engine_data_submission_url,
    ARRAY_AGG(city IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS city,
    ARRAY_AGG(geo_subdivision1 IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS subdivision1,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(country IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS country,
    ARRAY_AGG(os IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS normalized_os,
    ARRAY_AGG(normalized_os_version IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_os_version,
    ARRAY_AGG(attribution.dltoken IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS download_token
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  WHERE
    submission_date >= starting_date
  GROUP BY
    client_id
)
SELECT
  client_id,
  main.sample_id AS sample_id,
  DATE(
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            CAST(new_profile.submission_timestamp AS STRING),
            new_profile_ping_name,
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            CAST(shutdown.submission_timestamp AS STRING),
            shutdown_ping_name,
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (
          STRUCT(
            CAST(main.submission_timestamp AS STRING),
            main_ping_name,
            DATETIME(main.submission_timestamp)
          )
        )
      ]
    ).earliest_date
  ) AS first_seen_date,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          CAST(new_profile.submission_timestamp AS STRING),
          new_profile_ping_name,
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(
          CAST(shutdown.submission_timestamp AS STRING),
          shutdown_ping_name,
          DATETIME(shutdown.submission_timestamp)
        )
      ),
      (
        STRUCT(
          CAST(main.submission_timestamp AS STRING),
          main_ping_name,
          DATETIME(main.submission_timestamp)
        )
      )
    ]
  ).earliest_date AS submission_timestamp,
  new_profile.architecture AS architecture,
  main.app_build_id AS app_build_id,
  main.channel AS channel,
  main.app_display_version AS app_display_version,
  main.app_name AS app_name,
  new_profile.platform_version AS platform_version,
  main.vendor AS vendor,
  main.app_version AS app_version,
  new_profile.xpcom_abi AS xpcom_abi,
  new_profile.document_id AS document_id,
  new_profile.experiments_key AS experiments_key,
  new_profile.experiments_branch AS experiments_branch,
  new_profile.experiments_enrollment_id AS experiments_enrollment_id,
  new_profile.experiments_type AS experiments_type,
  main.distribution_id AS distribution_id,
  new_profile.partner_distribution_version AS partner_distribution_version,
  new_profile.partner_distributor AS partner_distributor,
  new_profile.partner_distributor_channel AS partner_distributor_channel,
  new_profile.partner_id AS partner_id,
  main.engine_data_load_path AS engine_data_load_path,
  main.engine_data_name AS engine_data_name,
  main.engine_data_origin AS engine_data_origin,
  main.engine_data_submission_url AS engine_data_submission_url,
  new_profile.apple_model_id AS apple_model_id,
  main.city AS city,
  main.subdivision1 AS subdivision1,
  main.normalized_channel AS normalized_channel,
  main.country AS country,
  main.normalized_os AS normalized_os,
  main.normalized_os_version AS normalized_os_version,
  new_profile.startup_profile_selection_reason AS startup_profile_selection_reason,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.download_token,
          new_profile_ping_name,
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(shutdown.download_token, shutdown_ping_name, DATETIME(shutdown.submission_timestamp))
      ),
      (STRUCT(main.download_token, main_ping_name, DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS download_token,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.download_source,
          new_profile_ping_name,
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(
          shutdown.download_source,
          shutdown_ping_name,
          DATETIME(shutdown.submission_timestamp)
        )
      )
    ]
  ).earliest_value AS download_source,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.attribution_campaign,
          new_profile_ping_name,
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(
          shutdown.attribution_campaign,
          shutdown_ping_name,
          DATETIME(shutdown.submission_timestamp)
        )
      ),
      (STRUCT(main.attribution_campaign, main_ping_name, DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS attribution_campaign,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.attribution_content,
          new_profile_ping_name,
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(
          shutdown.attribution_content,
          shutdown_ping_name,
          DATETIME(shutdown.submission_timestamp)
        )
      ),
      (STRUCT(main.attribution_content, main_ping_name, DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS attribution_content,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.attribution_experiment,
          new_profile_ping_name,
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(
          shutdown.attribution_experiment,
          shutdown_ping_name,
          DATETIME(shutdown.submission_timestamp)
        )
      ),
      (STRUCT(main.attribution_experiment, main_ping_name, DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS attribution_experiment,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.attribution_medium,
          new_profile_ping_name,
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(
          shutdown.attribution_medium,
          shutdown_ping_name,
          DATETIME(shutdown.submission_timestamp)
        )
      ),
      (STRUCT(main.attribution_medium, main_ping_name, DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS attribution_medium,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.attribution_source,
          new_profile_ping_name,
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(
          shutdown.attribution_source,
          shutdown_ping_name,
          DATETIME(shutdown.submission_timestamp)
        )
      ),
      (STRUCT(main.attribution_source, main_ping_name, DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS attribution_source,
  STRUCT(
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            CAST(new_profile.submission_timestamp AS STRING),
            new_profile_ping_name,
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            CAST(shutdown.submission_timestamp AS STRING),
            shutdown_ping_name,
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (
          STRUCT(
            CAST(main.submission_timestamp AS STRING),
            main_ping_name,
            DATETIME(main.submission_timestamp)
          )
        )
      ]
    ).earliest_value_source AS first_seen_date__source_ping,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_campaign,
            new_profile_ping_name,
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            shutdown.attribution_campaign,
            shutdown_ping_name,
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (STRUCT(main.attribution_campaign, main_ping_name, DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value_source AS attribution_campaign__source_ping,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.download_token,
            new_profile_ping_name,
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            shutdown.download_token,
            shutdown_ping_name,
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (STRUCT(main.download_token, main_ping_name, DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value_source AS download_token__source_ping,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.download_source,
            new_profile_ping_name,
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            shutdown.download_source,
            shutdown_ping_name,
            DATETIME(shutdown.submission_timestamp)
          )
        )
      ]
    ).earliest_value_source AS download_source__source_ping,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_content,
            new_profile_ping_name,
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            shutdown.attribution_content,
            shutdown_ping_name,
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (STRUCT(main.attribution_content, main_ping_name, DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value_source AS attribution_content__source_ping,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_experiment,
            new_profile_ping_name,
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            shutdown.attribution_experiment,
            shutdown_ping_name,
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (STRUCT(main.attribution_experiment, main_ping_name, DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value_source AS attribution_experiment__source_ping,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_medium,
            new_profile_ping_name,
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            shutdown.attribution_medium,
            shutdown_ping_name,
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (STRUCT(main.attribution_medium, main_ping_name, DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value_source AS attribution_medium__source_ping,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_source,
            new_profile_ping_name,
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            shutdown.attribution_source,
            shutdown_ping_name,
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (STRUCT(main.attribution_source, main_ping_name, DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value_source AS attribution_source__source_ping,
    CASE
      WHEN new_profile.client_id IS NULL
        THEN FALSE
      ELSE TRUE
    END AS reported_new_profile_ping,
    CASE
      WHEN shutdown.client_id IS NULL
        THEN FALSE
      ELSE TRUE
    END AS reported_shutdown_ping,
    CASE
      WHEN main.client_id IS NULL
        THEN FALSE
      ELSE TRUE
    END AS reported_main_ping
  ) AS metadata
FROM
  new_profile_ping AS new_profile
FULL OUTER JOIN
  shutdown_ping AS shutdown
USING
  (client_id)
FULL OUTER JOIN
  main_ping AS main
USING
  (client_id)
WHERE
  client_id IS NOT NULL
