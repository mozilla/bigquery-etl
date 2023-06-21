-- Query to initialize clients_first_seen_v2
-- Earliest ping dates for each ping used in the query.
-- DECLARE new_profile_ping_earliest_date DATE DEFAULT '2017-06-26';

-- DECLARE shutdown_ping_earliest_date DATE DEFAULT '2018-10-30';

-- DECLARE main_ping_earliest_date DATE DEFAULT '2016-03-12';

DECLARE new_profile_ping_earliest_date DATE DEFAULT '2023-06-01';

DECLARE shutdown_ping_earliest_date DATE DEFAULT '2023-06-01';

DECLARE main_ping_earliest_date DATE DEFAULT '2023-06-01';

-- Names of pings to store in the metadata.
DECLARE new_profile_ping_name STRING DEFAULT 'new_profile';

DECLARE shutdown_ping_name STRING DEFAULT 'shutdown';

DECLARE main_ping_name STRING DEFAULT 'main';

CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.analysis.clients_first_seen_v2_jun21`
PARTITION BY
  first_seen_date
AS
WITH new_profile_ping AS (
  SELECT
    client_id AS client_id,
    MIN(sample_id) AS sample_id,
    MIN(submission_timestamp) AS submission_timestamp,
    ARRAY_AGG(application.architecture IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS architecture,
    ARRAY_AGG(application.build_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS app_build_id,
    ARRAY_AGG(application.channel IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(normalized_app_name IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS app_name,
    ARRAY_AGG(application.platform_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS platform_version,
    ARRAY_AGG(application.vendor IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS vendor,
    ARRAY_AGG(application.version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS app_version,
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
    ARRAY_AGG(environment.partner.distribution_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS distribution_id,
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
    ARRAY_AGG(
      environment.settings.default_search_engine_data.load_path IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS engine_data_load_path,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.name IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS engine_data_name,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.origin IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS engine_data_origin,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.submission_url IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS engine_data_submission_url,
    ARRAY_AGG(environment.system.apple_model_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS apple_model_id,
    ARRAY_AGG(metadata.geo.city IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS city,
    ARRAY_AGG(metadata.geo.db_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS db_version,
    ARRAY_AGG(metadata.geo.subdivision1 IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS subdivision1,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(normalized_country_code IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS country,
    ARRAY_AGG(normalized_os IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_os,
    ARRAY_AGG(normalized_os_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_os_version,
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
    DATE(submission_timestamp) >= new_profile_ping_earliest_date
  GROUP BY
    client_id
),
shutdown_ping AS (
  SELECT
    client_id AS client_id,
    MIN(sample_id) AS sample_id,
    MIN(submission_timestamp) AS submission_timestamp,
    ARRAY_AGG(application.architecture IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS architecture,
    ARRAY_AGG(application.build_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS app_build_id,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(normalized_app_name IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS app_name,
    ARRAY_AGG(application.platform_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS platform_version,
    ARRAY_AGG(application.vendor IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS vendor,
    ARRAY_AGG(application.version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS app_version,
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
    ARRAY_AGG(environment.partner.distribution_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS distribution_id,
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
    ARRAY_AGG(
      environment.settings.default_search_engine_data.load_path IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS engine_data_load_path,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.name IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS engine_data_name,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.origin IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS engine_data_origin,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.submission_url IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS engine_data_submission_url,
    ARRAY_AGG(environment.system.apple_model_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS apple_model_id,
    ARRAY_AGG(metadata.geo.city IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS city,
    ARRAY_AGG(metadata.geo.db_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS db_version,
    ARRAY_AGG(metadata.geo.subdivision1 IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS subdivision1,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(normalized_country_code IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS country,
    ARRAY_AGG(normalized_os IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_os,
    ARRAY_AGG(normalized_os_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_os_version,
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
    telemetry.first_shutdown
  LEFT JOIN
    UNNEST(environment.experiments) AS experiments
  WHERE
    DATE(submission_timestamp) >= shutdown_ping_earliest_date
  GROUP BY
    client_id
),
main_ping AS (
  SELECT
    client_id AS client_id,
    MIN(sample_id) AS sample_id,
    MIN(submission_timestamp_min) AS submission_timestamp,
    ARRAY_AGG(app_build_id IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS app_build_id,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(app_name IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS app_name,
    ARRAY_AGG(vendor IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS vendor,
    ARRAY_AGG(app_version IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS app_version,
    ARRAY_AGG(experiments.key IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS experiments_key,
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
    telemetry.clients_daily_v6
  LEFT JOIN
    UNNEST(experiments) AS experiments
  WHERE
    submission_date >= main_ping_earliest_date
  GROUP BY
    client_id
)
SELECT
  client_id,
  CAST(
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            CAST(new_profile.sample_id AS STRING),
            '',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (STRUCT(CAST(shutdown.sample_id AS STRING), '', DATETIME(shutdown.submission_timestamp))),
        (STRUCT(CAST(main.sample_id AS STRING), '', DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value AS INT
  ) AS sample_id,
  DATE(
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            CAST(new_profile.submission_timestamp AS STRING),
            '',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            CAST(shutdown.submission_timestamp AS STRING),
            '',
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (STRUCT(CAST(main.submission_timestamp AS STRING), '', DATETIME(main.submission_timestamp)))
      ]
    ).earliest_date
  ) AS first_seen_date,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          CAST(new_profile.submission_timestamp AS STRING),
          '',
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(
          CAST(shutdown.submission_timestamp AS STRING),
          '',
          DATETIME(shutdown.submission_timestamp)
        )
      ),
      (STRUCT(CAST(main.submission_timestamp AS STRING), '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_date AS submission_timestamp,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.architecture, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.architecture, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS architecture,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.app_build_id, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.app_build_id, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.app_build_id, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS app_build_id,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.channel, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.channel, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.channel, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS channel,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.app_name, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.app_name, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.app_name, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS app_name,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.platform_version, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.platform_version, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS platform_version,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.vendor, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.vendor, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.vendor, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS vendor,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.app_version, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.app_version, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.app_version, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS app_version,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.xpcom_abi, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.xpcom_abi, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS xpcom_abi,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.document_id, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.document_id, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS document_id,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.experiments_key, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.experiments_key, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.experiments_key, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS experiments_key,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.experiments_branch, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.experiments_branch, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS experiments_branch,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.experiments_enrollment_id,
          '',
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (STRUCT(shutdown.experiments_enrollment_id, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS experiments_enrollment_id,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.experiments_type, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.experiments_type, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS experiments_type,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.distribution_id, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.distribution_id, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.distribution_id, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS distribution_id,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.partner_distribution_version,
          '',
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (STRUCT(shutdown.partner_distribution_version, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS partner_distribution_version,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.partner_distributor, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.partner_distributor, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS partner_distributor,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.partner_distributor_channel,
          '',
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (STRUCT(shutdown.partner_distributor_channel, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS partner_distributor_channel,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.partner_id, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.partner_id, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS partner_id,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.engine_data_load_path, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.engine_data_load_path, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.engine_data_load_path, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS engine_data_load_path,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.engine_data_name, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.engine_data_name, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.engine_data_name, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS engine_data_name,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.engine_data_origin, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.engine_data_origin, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.engine_data_origin, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS engine_data_origin,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.engine_data_submission_url,
          '',
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (STRUCT(shutdown.engine_data_submission_url, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.engine_data_submission_url, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS engine_data_submission_url,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.apple_model_id, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.apple_model_id, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS apple_model_id,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.city, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.city, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.city, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS city,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.subdivision1, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.subdivision1, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.subdivision1, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS subdivision1,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.normalized_channel, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.normalized_channel, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.normalized_channel, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS normalized_channel,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.country, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.country, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.country, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS country,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.normalized_os, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.normalized_os, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.normalized_os, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS normalized_os,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.normalized_os_version, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.normalized_os_version, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.normalized_os_version, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS normalized_os_version,
  mozfun.norm.get_earliest_value(
    [
      (
        STRUCT(
          new_profile.startup_profile_selection_reason,
          '',
          DATETIME(new_profile.submission_timestamp)
        )
      ),
      (
        STRUCT(
          shutdown.startup_profile_selection_reason,
          '',
          DATETIME(shutdown.submission_timestamp)
        )
      )
    ]
  ).earliest_value AS startup_profile_selection_reason,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.download_token, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.download_token, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.download_token, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS download_token,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.download_source, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.download_source, '', DATETIME(shutdown.submission_timestamp)))
    ]
  ).earliest_value AS download_source,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.attribution_campaign, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.attribution_campaign, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.attribution_campaign, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS attribution_campaign,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.attribution_content, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.attribution_content, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.attribution_content, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS attribution_content,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.attribution_experiment, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.attribution_experiment, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.attribution_experiment, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS attribution_experiment,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.attribution_medium, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.attribution_medium, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.attribution_medium, '', DATETIME(main.submission_timestamp)))
    ]
  ).earliest_value AS attribution_medium,
  mozfun.norm.get_earliest_value(
    [
      (STRUCT(new_profile.attribution_source, '', DATETIME(new_profile.submission_timestamp))),
      (STRUCT(shutdown.attribution_source, '', DATETIME(shutdown.submission_timestamp))),
      (STRUCT(main.attribution_source, '', DATETIME(main.submission_timestamp)))
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
