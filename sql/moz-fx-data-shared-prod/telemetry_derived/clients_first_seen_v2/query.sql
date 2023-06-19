-- Query for firefox_desktop_derived.firefox_desktop_clients_v1
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
    DATE(submission_timestamp) = @submission_date
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
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
),
main_ping AS (
  SELECT
    client_id AS client_id,
    MIN(sample_id) AS sample_id,
    TIMESTAMP(MIN(submission_date)) AS submission_timestamp,
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
    submission_date = @submission_date
  GROUP BY
    client_id
),
_current AS (
  SELECT
    client_id,
    main.sample_id AS sample_id,
    DATE(
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              CAST(new_profile.submission_timestamp AS STRING),
              'new_profile',
              DATETIME(new_profile.submission_timestamp)
            )
          ),
          (
            STRUCT(
              CAST(shutdown.submission_timestamp AS STRING),
              'shutdown',
              DATETIME(shutdown.submission_timestamp)
            )
          ),
          (
            STRUCT(
              CAST(main.submission_timestamp AS STRING),
              'main',
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
            'new_profile',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            CAST(shutdown.submission_timestamp AS STRING),
            'shutdown',
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (
          STRUCT(
            CAST(main.submission_timestamp AS STRING),
            'main',
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
            'new_profile',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (STRUCT(shutdown.download_token, 'shutdown', DATETIME(shutdown.submission_timestamp))),
        (STRUCT(main.download_token, 'main', DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value AS download_token,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.download_source,
            'new_profile',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (STRUCT(shutdown.download_source, 'shutdown', DATETIME(shutdown.submission_timestamp)))
      ]
    ).earliest_value AS download_source,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_campaign,
            'new_profile',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(shutdown.attribution_campaign, 'shutdown', DATETIME(shutdown.submission_timestamp))
        ),
        (STRUCT(main.attribution_campaign, 'main', DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value AS attribution_campaign,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_content,
            'new_profile',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (STRUCT(shutdown.attribution_content, 'shutdown', DATETIME(shutdown.submission_timestamp))),
        (STRUCT(main.attribution_content, 'main', DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value AS attribution_content,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_experiment,
            'new_profile',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (
          STRUCT(
            shutdown.attribution_experiment,
            'shutdown',
            DATETIME(shutdown.submission_timestamp)
          )
        ),
        (STRUCT(main.attribution_experiment, 'main', DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value AS attribution_experiment,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_medium,
            'new_profile',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (STRUCT(shutdown.attribution_medium, 'shutdown', DATETIME(shutdown.submission_timestamp))),
        (STRUCT(main.attribution_medium, 'main', DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value AS attribution_medium,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            new_profile.attribution_source,
            'new_profile',
            DATETIME(new_profile.submission_timestamp)
          )
        ),
        (STRUCT(shutdown.attribution_source, 'shutdown', DATETIME(shutdown.submission_timestamp))),
        (STRUCT(main.attribution_source, 'main', DATETIME(main.submission_timestamp)))
      ]
    ).earliest_value AS attribution_source,
    STRUCT(
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              CAST(new_profile.submission_timestamp AS STRING),
              'new_profile',
              DATETIME(new_profile.submission_timestamp)
            )
          ),
          (
            STRUCT(
              CAST(shutdown.submission_timestamp AS STRING),
              'shutdown',
              DATETIME(shutdown.submission_timestamp)
            )
          ),
          (
            STRUCT(
              CAST(main.submission_timestamp AS STRING),
              'main',
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
              'new_profile',
              DATETIME(new_profile.submission_timestamp)
            )
          ),
          (
            STRUCT(
              shutdown.attribution_campaign,
              'shutdown',
              DATETIME(shutdown.submission_timestamp)
            )
          ),
          (STRUCT(main.attribution_campaign, 'main', DATETIME(main.submission_timestamp)))
        ]
      ).earliest_value_source AS attribution_campaign__source_ping,
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              new_profile.download_token,
              'new_profile',
              DATETIME(new_profile.submission_timestamp)
            )
          ),
          (STRUCT(shutdown.download_token, 'shutdown', DATETIME(shutdown.submission_timestamp))),
          (STRUCT(main.download_token, 'main', DATETIME(main.submission_timestamp)))
        ]
      ).earliest_value_source AS download_token__source_ping,
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              new_profile.download_source,
              'new_profile',
              DATETIME(new_profile.submission_timestamp)
            )
          ),
          (STRUCT(shutdown.download_source, 'shutdown', DATETIME(shutdown.submission_timestamp)))
        ]
      ).earliest_value_source AS download_source__source_ping,
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              new_profile.attribution_content,
              'new_profile',
              DATETIME(new_profile.submission_timestamp)
            )
          ),
          (
            STRUCT(
              shutdown.attribution_content,
              'shutdown',
              DATETIME(shutdown.submission_timestamp)
            )
          ),
          (STRUCT(main.attribution_content, 'main', DATETIME(main.submission_timestamp)))
        ]
      ).earliest_value_source AS attribution_content__source_ping,
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              new_profile.attribution_experiment,
              'new_profile',
              DATETIME(new_profile.submission_timestamp)
            )
          ),
          (
            STRUCT(
              shutdown.attribution_experiment,
              'shutdown',
              DATETIME(shutdown.submission_timestamp)
            )
          ),
          (STRUCT(main.attribution_experiment, 'main', DATETIME(main.submission_timestamp)))
        ]
      ).earliest_value_source AS attribution_experiment__source_ping,
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              new_profile.attribution_medium,
              'new_profile',
              DATETIME(new_profile.submission_timestamp)
            )
          ),
          (
            STRUCT(shutdown.attribution_medium, 'shutdown', DATETIME(shutdown.submission_timestamp))
          ),
          (STRUCT(main.attribution_medium, 'main', DATETIME(main.submission_timestamp)))
        ]
      ).earliest_value_source AS attribution_medium__source_ping,
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              new_profile.attribution_source,
              'new_profile',
              DATETIME(new_profile.submission_timestamp)
            )
          ),
          (
            STRUCT(shutdown.attribution_source, 'shutdown', DATETIME(shutdown.submission_timestamp))
          ),
          (STRUCT(main.attribution_source, 'main', DATETIME(main.submission_timestamp)))
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
),
_previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2`
)
SELECT
  client_id,
  COALESCE(_previous.sample_id, _current.sample_id) AS sample_id,
  COALESCE(_previous.first_seen_date, _current.first_seen_date) AS first_seen_date,
  COALESCE(_previous.submission_timestamp, _current.submission_timestamp) AS submission_timestamp,
  COALESCE(_previous.architecture, _current.architecture) AS architecture,
  COALESCE(_previous.app_build_id, _current.app_build_id) AS app_build_id,
  COALESCE(_previous.app_display_version, _current.app_display_version) AS app_display_version,
  COALESCE(_previous.app_name, _current.app_name) AS app_name,
  COALESCE(_previous.vendor, _current.vendor) AS vendor,
  COALESCE(_previous.app_version, _current.app_version) AS app_version,
  COALESCE(_previous.xpcom_abi, _current.xpcom_abi) AS xpcom_abi,
  COALESCE(_previous.experiments_key, _current.experiments_key) AS experiments_key,
  COALESCE(_previous.experiments_branch, _current.experiments_branch) AS experiments_branch,
  COALESCE(
    _previous.experiments_enrollment_id,
    _current.experiments_enrollment_id
  ) AS experiments_enrollment_id,
  COALESCE(_previous.experiments_type, _current.experiments_type) AS experiments_type,
  COALESCE(_previous.document_id, _current.document_id) AS document_id,
  COALESCE(_previous.distribution_id, _current.distribution_id) AS distribution_id,
  COALESCE(
    _previous.partner_distribution_version,
    _current.partner_distribution_version
  ) AS partner_distribution_version,
  COALESCE(_previous.partner_distributor, _current.partner_distributor) AS partner_distributor,
  COALESCE(
    _previous.partner_distributor_channel,
    _current.partner_distributor_channel
  ) AS partner_distributor_channel,
  COALESCE(_previous.partner_id, _current.partner_id) AS partner_id,
  COALESCE(_previous.attribution_campaign, _current.attribution_campaign) AS attribution_campaign,
  COALESCE(_previous.attribution_content, _current.attribution_content) AS attribution_content,
  COALESCE(
    _previous.attribution_experiment,
    _current.attribution_experiment
  ) AS attribution_experiment,
  COALESCE(_previous.attribution_medium, _current.attribution_medium) AS attribution_medium,
  COALESCE(_previous.attribution_source, _current.attribution_source) AS attribution_source,
  COALESCE(
    _previous.engine_data_load_path,
    _current.engine_data_load_path
  ) AS engine_data_load_path,
  COALESCE(_previous.engine_data_name, _current.engine_data_name) AS engine_data_name,
  COALESCE(_previous.engine_data_origin, _current.engine_data_origin) AS engine_data_origin,
  COALESCE(
    _previous.engine_data_submission_url,
    _current.engine_data_submission_url
  ) AS engine_data_submission_url,
  COALESCE(_previous.apple_model_id, _current.apple_model_id) AS apple_model_id,
  COALESCE(_previous.city, _current.city) AS city,
  COALESCE(_previous.subdivision1, _current.subdivision1) AS subdivision1,
  COALESCE(_previous.normalized_channel, _current.normalized_channel) AS normalized_channel,
  COALESCE(_previous.country, _current.country) AS country,
  COALESCE(_previous.normalized_os, _current.normalized_os) AS normalized_os,
  COALESCE(
    _previous.normalized_os_version,
    _current.normalized_os_version
  ) AS normalized_os_version,
  COALESCE(
    _previous.startup_profile_selection_reason,
    _current.startup_profile_selection_reason
  ) AS startup_profile_selection_reason,
  COALESCE(_previous.download_token, _current.download_token) AS download_token,
  COALESCE(_previous.download_source, _current.download_source) AS download_source,
  STRUCT(
    COALESCE(
      _previous.metadata.first_seen_date__source_ping,
      _current.metadata.first_seen_date__source_ping
    ) AS first_seen_date__source_ping,
    COALESCE(
      _previous.metadata.attribution_campaign__source_ping,
      _current.metadata.attribution_campaign__source_ping
    ) AS attribution_campaign__source_ping,
    COALESCE(
      _previous.metadata.attribution_content__source_ping,
      _current.metadata.attribution_content__source_ping
    ) AS attribution_content__source_ping,
    COALESCE(
      _previous.metadata.attribution_experiment__source_ping,
      _current.metadata.attribution_experiment__source_ping
    ) AS attribution_experiment__source_ping,
    COALESCE(
      _previous.metadata.attribution_medium__source_ping,
      _current.metadata.attribution_medium__source_ping
    ) AS attribution_medium__source_ping,
    COALESCE(
      _previous.metadata.attribution_source__source_ping,
      _current.metadata.attribution_source__source_ping
    ) AS attribution_source__source_ping,
    COALESCE(
      _previous.metadata.download_token__source_ping,
      _current.metadata.download_token__source_ping
    ) AS download_token__source_ping,
    COALESCE(
      _previous.metadata.download_source__source_ping,
      _current.metadata.download_source__source_ping
    ) AS download_source__source_ping,
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
    ) AS reported_main_ping
  ) AS metadata
FROM
  _previous
FULL JOIN
  _current
USING
  (client_id)
