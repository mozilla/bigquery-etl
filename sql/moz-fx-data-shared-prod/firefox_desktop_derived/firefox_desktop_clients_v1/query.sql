-- Query for firefox_desktop_derived.firefox_desktop_clients_v1

-- TODO:
-- add attributes from new_profile or from main? /Run query
-- add submission_date / yes
-- add second_seen_date? / Leif
-- add dlsource? / George
-- Update over time? % of change < 0,5% / yes
-- Update _previous with dataset firefox_desktop_derived / Lucia

WITH new_profile_ping AS
(
    SELECT
    client_id AS client_id,
    MIN(sample_id) AS sample_id,
    MIN(submission_timestamp) AS submission_datetime,
    ARRAY_AGG(application.architecture IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS architecture,
    ARRAY_AGG(application.build_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS app_build_id,
    ARRAY_AGG(application.channel IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(application.display_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS app_display_version,
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
    ARRAY_AGG(environment.partner.distribution_version IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS partner_distribution_version,
    ARRAY_AGG(environment.partner.distributor IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS partner_distributor,
    ARRAY_AGG(environment.partner.distributor_channel IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS partner_distributor_channel,
    ARRAY_AGG(environment.partner.partner_id IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS partner_id,
    ARRAY_AGG(environment.settings.attribution.campaign IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_campaign,
    ARRAY_AGG(environment.settings.attribution.content IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_content,
    ARRAY_AGG(environment.settings.attribution.experiment IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_experiment,
    ARRAY_AGG(environment.settings.attribution.medium IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_medium,
    ARRAY_AGG(environment.settings.attribution.source IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_source,
    ARRAY_AGG(environment.settings.default_search_engine_data.load_path IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS engine_data_load_path,
    ARRAY_AGG(environment.settings.default_search_engine_data.name IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS engine_data_name,
    ARRAY_AGG(environment.settings.default_search_engine_data.origin IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS engine_data_origin,
    ARRAY_AGG(environment.settings.default_search_engine_data.submission_url IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS engine_data_submission_url,
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
    ARRAY_AGG(payload.processes.parent.scalars.startup_profile_selection_reason IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS startup_profile_selection_reason,
    ARRAY_AGG(environment.settings.attribution.dltoken IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS download_token
  FROM
    telemetry.new_profile
    LEFT JOIN UNNEST(environment.experiments) as experiments
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY client_id
),
shutdown_ping AS
(
  SELECT
    client_id AS client_id,
    MIN(submission_timestamp) AS submission_datetime,
    ARRAY_AGG(environment.settings.attribution.campaign IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_campaign,
    ARRAY_AGG(environment.settings.attribution.content IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_content,
    ARRAY_AGG(environment.settings.attribution.experiment IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_experiment,
    ARRAY_AGG(environment.settings.attribution.medium IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_medium,
    ARRAY_AGG(environment.settings.attribution.source IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS attribution_source,
    ARRAY_AGG(environment.settings.attribution.dltoken IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS download_token
  FROM
    telemetry.first_shutdown
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY client_id
),
main_ping AS
(
  SELECT
    client_id AS client_id,
    TIMESTAMP(MIN(submission_date)) AS submission_datetime,
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
    ARRAY_AGG(attribution.dltoken IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS download_token
  FROM
    telemetry.clients_daily
  WHERE
    submission_date = @submission_date
  GROUP BY client_id
),
_current AS(
  SELECT
    client_id,
    new_profile.sample_id AS sample_id,
    DATE (
      analysis.get_earliest_value(
        [
        (STRUCT(CAST(new_profile.submission_datetime AS STRING), 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
        (STRUCT(CAST(shutdown.submission_datetime AS STRING), 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
        (STRUCT(CAST(main.submission_datetime AS STRING), 'main_ping', DATETIME(main.submission_datetime)))
        ]
      ).earliest_date
    ) AS first_seen_date,
    new_profile.architecture AS architecture,
    new_profile.app_build_id AS app_build_id,
    new_profile.channel AS channel,
    new_profile.app_display_version AS app_display_version,
    new_profile.app_name AS app_name,
    new_profile.vendor AS vendor,
    new_profile.app_version AS app_version,
    new_profile.xpcom_abi AS xpcom_abi,
    new_profile.document_id AS document_id,
    new_profile.experiments_key AS experiments_key,
    new_profile.experiments_branch AS experiments_branch,
    new_profile.experiments_enrollment_id AS experiments_enrollment_id,
    new_profile.experiments_type AS experiments_type,
    new_profile.distribution_id AS distribution_id,
    new_profile.partner_distribution_version AS partner_distribution_version,
    new_profile.partner_distributor AS partner_distributor,
    new_profile.partner_distributor_channel AS partner_distributor_channel,
    new_profile.partner_id AS partner_id,
    new_profile.engine_data_load_path AS engine_data_load_path,
    new_profile.engine_data_name AS engine_data_name,
    new_profile.engine_data_origin AS engine_data_origin,
    new_profile.engine_data_submission_url AS engine_data_submission_url,
    new_profile.apple_model_id AS apple_model_id,
    new_profile.city AS city,
    new_profile.subdivision1 AS subdivision1,
    new_profile.normalized_channel AS normalized_channel,
    new_profile.country AS country,
    new_profile.normalized_os AS normalized_os,
    new_profile.normalized_os_version AS normalized_os_version,
    new_profile.startup_profile_selection_reason AS startup_profile_selection_reason,
    new_profile.download_token AS download_token,
    analysis.get_earliest_value(
      [
        (STRUCT(new_profile.attribution_campaign, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
        (STRUCT(shutdown.attribution_campaign, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
        (STRUCT(main.attribution_campaign, 'main_ping', DATETIME(main.submission_datetime)))
      ]
    ).earliest_value AS attribution_campaign,
    analysis.get_earliest_value(
      [
        (STRUCT(new_profile.attribution_content, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
        (STRUCT(shutdown.attribution_content, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
        (STRUCT(main.attribution_content, 'main_ping', DATETIME(main.submission_datetime)))
      ]
    ).earliest_value AS attribution_content,
    analysis.get_earliest_value(
      [
        (STRUCT(new_profile.attribution_experiment, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
        (STRUCT(shutdown.attribution_experiment, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
        (STRUCT(main.attribution_experiment, 'main_ping', DATETIME(main.submission_datetime)))
      ]
    ).earliest_value AS attribution_experiment,
    analysis.get_earliest_value(
      [
        (STRUCT(new_profile.attribution_medium, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
        (STRUCT(shutdown.attribution_medium, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
        (STRUCT(main.attribution_medium, 'main_ping', DATETIME(main.submission_datetime)))
      ]
    ).earliest_value AS attribution_medium,
    analysis.get_earliest_value(
      [
        (STRUCT(new_profile.attribution_source, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
        (STRUCT(shutdown.attribution_source, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
        (STRUCT(main.attribution_source, 'main_ping', DATETIME(main.submission_datetime)))
      ]
    ).earliest_value AS attribution_source,
    analysis.get_earliest_value(
      [
        (STRUCT(new_profile.download_token, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
        (STRUCT(shutdown.download_token, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
        (STRUCT(main.download_token, 'main_ping', DATETIME(main.submission_datetime)))
      ]
    ).earliest_value AS download_token_source,
    STRUCT(
      analysis.get_earliest_value(
        [
          (STRUCT(CAST(new_profile.submission_datetime AS STRING), 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
          (STRUCT(CAST(shutdown.submission_datetime AS STRING), 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
          (STRUCT(CAST(shutdown.submission_datetime AS STRING), 'main_ping', DATETIME(main.submission_datetime)))
        ]
        ).earliest_value_source AS first_seen_date_source,
      analysis.get_earliest_value(
        [
          (STRUCT(new_profile.attribution_campaign, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
          (STRUCT(shutdown.attribution_campaign, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
          (STRUCT(main.attribution_campaign, 'main_ping', DATETIME(main.submission_datetime)))
        ]
      ).earliest_value_source AS attribution_campaign_source,
      analysis.get_earliest_value(
        [
          (STRUCT(new_profile.attribution_content, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
          (STRUCT(shutdown.attribution_content, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
          (STRUCT(main.attribution_content, 'main_ping', DATETIME(main.submission_datetime)))
        ]
      ).earliest_value_source AS attribution_content_source,
      analysis.get_earliest_value(
        [
          (STRUCT(new_profile.attribution_experiment, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
          (STRUCT(shutdown.attribution_experiment, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
          (STRUCT(main.attribution_experiment, 'main_ping', DATETIME(main.submission_datetime)))
        ]
      ).earliest_value_source AS attribution_experiment_source,
      analysis.get_earliest_value(
        [
          (STRUCT(new_profile.attribution_medium, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
          (STRUCT(shutdown.attribution_medium, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
          (STRUCT(main.attribution_medium, 'main_ping', DATETIME(main.submission_datetime)))
        ]
      ).earliest_value_source AS attribution_medium_source,
      analysis.get_earliest_value(
        [
          (STRUCT(new_profile.attribution_source, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
          (STRUCT(shutdown.attribution_source, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
          (STRUCT(main.attribution_source, 'main_ping', DATETIME(main.submission_datetime)))
        ]
      ).earliest_value_source AS attribution_source_source,
      analysis.get_earliest_value(
        [
          (STRUCT(new_profile.download_token, 'new_profile_ping', DATETIME(new_profile.submission_datetime))),
          (STRUCT(shutdown.download_token, 'shutdown_ping', DATETIME(shutdown.submission_datetime))),
          (STRUCT(main.download_token, 'main_ping', DATETIME(main.submission_datetime)))
        ]
      ).earliest_value_source AS download_token_source,
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
      END AS reported_main_ping,
      CURRENT_DATE() AS last_updated_date
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
),
_previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.analysis.firefox_desktop_clients_v1`
)
SELECT
  client_id,
  COALESCE(_previous.sample_id, _current.sample_id) AS sample_id,
  COALESCE(_previous.first_seen_date, _current.first_seen_date) AS first_seen_date,
  COALESCE(_previous.architecture, _current.architecture) AS architecture,
  COALESCE(_previous.app_build_id, _current.app_build_id) AS app_build_id,
  COALESCE(_previous.app_display_version, _current.app_display_version) AS app_display_version,
  COALESCE(_previous.app_name, _current.app_name) AS app_name,
  COALESCE(_previous.vendor, _current.vendor) AS vendor,
  COALESCE(_previous.app_version, _current.app_version) AS app_version,
  COALESCE(_previous.xpcom_abi, _current.xpcom_abi) AS xpcom_abi,
  COALESCE(_previous.document_id, _current.document_id) AS document_id,
  COALESCE(_previous.experiments_key, _current.experiments_key) AS experiments_key,
  COALESCE(_previous.experiments_branch, _current.experiments_branch) AS experiments_branch,
  COALESCE(_previous.experiments_enrollment_id, _current.experiments_enrollment_id) AS experiments_enrollment_id,
  COALESCE(_previous.experiments_type, _current.experiments_type) AS experiments_type,
  COALESCE(_previous.distribution_id, _current.distribution_id) AS distribution_id,
  COALESCE(_previous.partner_distribution_version, _current.partner_distribution_version) AS partner_distribution_version,
  COALESCE(_previous.partner_distributor, _current.partner_distributor) AS partner_distributor,
  COALESCE(_previous.partner_distributor_channel, _current.partner_distributor_channel) AS partner_distributor_channel,
  COALESCE(_previous.partner_id, _current.partner_id) AS partner_id,
  COALESCE(_previous.attribution_campaign, _current.attribution_campaign) AS attribution_campaign,
  COALESCE(_previous.attribution_content, _current.attribution_content) AS attribution_content,
  COALESCE(_previous.attribution_experiment, _current.attribution_experiment) AS attribution_experiment,
  COALESCE(_previous.attribution_medium, _current.attribution_medium) AS attribution_medium,
  COALESCE(_previous.attribution_source, _current.attribution_source) AS attribution_source,
  COALESCE(_previous.engine_data_load_path, _current.engine_data_load_path) AS engine_data_load_path,
  COALESCE(_previous.engine_data_name, _current.engine_data_name) AS engine_data_name,
  COALESCE(_previous.engine_data_origin, _current.engine_data_origin) AS engine_data_origin,
  COALESCE(_previous.engine_data_submission_url, _current.engine_data_submission_url) AS engine_data_submission_url,
  COALESCE(_previous.apple_model_id, _current.apple_model_id) AS apple_model_id,
  COALESCE(_previous.city, _current.city) AS city,
  COALESCE(_previous.subdivision1, _current.subdivision1) AS subdivision1,
  COALESCE(_previous.normalized_channel, _current.normalized_channel) AS normalized_channel,
  COALESCE(_previous.country, _current.country) AS country,
  COALESCE(_previous.normalized_os, _current.normalized_os) AS normalized_os,
  COALESCE(_previous.normalized_os_version, _current.normalized_os_version) AS normalized_os_version,
  COALESCE(_previous.startup_profile_selection_reason, _current.startup_profile_selection_reason) AS startup_profile_selection_reason,
  COALESCE(_previous.download_token, _current.download_token) AS download_token,
  STRUCT(
    COALESCE(
        _previous.metadata.first_seen_date_source,
        _current.metadata.first_seen_date_source
      ) AS first_seen_date_source,
    COALESCE(
          _previous.metadata.attribution_campaign_source,
          _current.metadata.attribution_campaign_source
        ) AS attribution_campaign_source,
    COALESCE(
          _previous.metadata.attribution_content_source,
          _current.metadata.attribution_content_source
        ) AS attribution_content_source,
    COALESCE(
          _previous.metadata.attribution_experiment_source,
          _current.metadata.attribution_experiment_source
        ) AS attribution_experiment_source,
    COALESCE(
          _previous.metadata.attribution_medium_source,
          _current.metadata.attribution_medium_source
        ) AS attribution_medium_source,
    COALESCE(
          _previous.metadata.attribution_source_source,
          _current.metadata.attribution_source_source
        ) AS attribution_source_source,
    COALESCE(
          _previous.metadata.download_token_source,
          _current.metadata.download_token_source
        ) AS download_token_source,
    COALESCE(
          _previous.metadata.reported_new_profile_ping,
          _current.metadata.reported_new_profile_ping
        ) AS reported_new_profile_ping,
    COALESCE(
          _previous.metadata.reported_shutdown_ping,
          _current.metadata.reported_shutdown_ping
        ) AS reported_shutdown_ping,
    COALESCE(
          _previous.metadata.reported_main_ping,
          _current.metadata.reported_main_ping
        ) AS reported_main_ping,
    COALESCE(
          _previous.metadata.last_updated_date,
          _current.metadata.last_updated_date
        ) AS last_updated_date
  ) AS metadata
FROM
  _previous
FULL JOIN
  _current
USING
  (client_id)