-- Query for telemetry_derived.clients_first_seen_v2
{% if is_init() %}
INSERT INTO
 `{project_id}.{dataset_id}.{table_id}`
{% endif %}
-- Each ping type subquery retrieves all attributes as reported on the first
-- ping received and respecting NULLS.
-- Once the first_seen_date is identified after comparing all pings, attributes
-- are retrieved for each client_id from the ping type that reported it.
WITH new_profile_ping AS (
  SELECT
    client_id AS client_id,
    sample_id AS sample_id,
    MIN(submission_timestamp) AS first_seen_timestamp,
    ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp ASC) AS all_dates,
    ARRAY_AGG(application.architecture RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS architecture,
    ARRAY_AGG(environment.build.build_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS app_build_id,
    ARRAY_AGG(normalized_app_name RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS app_name,
    ARRAY_AGG(environment.settings.locale RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS locale,
    ARRAY_AGG(application.platform_version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS platform_version,
    ARRAY_AGG(application.vendor RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS vendor,
    ARRAY_AGG(application.version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS app_version,
    ARRAY_AGG(application.xpcom_abi RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS xpcom_abi,
    ARRAY_AGG(document_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS document_id,
    ARRAY_AGG(environment.partner.distribution_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS distribution_id,
    ARRAY_AGG(environment.partner.distribution_version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS partner_distribution_version,
    ARRAY_AGG(environment.partner.distributor RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS partner_distributor,
    ARRAY_AGG(environment.partner.distributor_channel RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS partner_distributor_channel,
    ARRAY_AGG(environment.partner.partner_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS partner_id,
    ARRAY_AGG(
      environment.settings.attribution.campaign RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS attribution_campaign,
    ARRAY_AGG(environment.settings.attribution.content RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_content,
    ARRAY_AGG(
      environment.settings.attribution.experiment RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS attribution_experiment,
    ARRAY_AGG(environment.settings.attribution.medium RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_medium,
    ARRAY_AGG(environment.settings.attribution.source RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_source,
    ARRAY_AGG(environment.settings.attribution.ua RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_ua,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.load_path RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS engine_data_load_path,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.name RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS engine_data_name,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.origin RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS engine_data_origin,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.submission_url RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS engine_data_submission_url,
    ARRAY_AGG(environment.system.apple_model_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS apple_model_id,
    ARRAY_AGG(metadata.geo.city RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS city,
    ARRAY_AGG(metadata.geo.db_version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS db_version,
    ARRAY_AGG(metadata.geo.subdivision1 RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS subdivision1,
    ARRAY_AGG(normalized_channel RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(normalized_country_code RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS country,
    ARRAY_AGG(normalized_os RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS normalized_os,
    ARRAY_AGG(normalized_os_version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS normalized_os_version,
    ARRAY_AGG(
      payload.processes.parent.scalars.startup_profile_selection_reason RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS startup_profile_selection_reason,
    ARRAY_AGG(environment.settings.attribution.dltoken RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_dltoken,
    ARRAY_AGG(
      environment.settings.attribution.dlsource RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS attribution_dlsource,
  FROM
    `moz-fx-data-shared-prod.telemetry.new_profile`
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= '2010-01-01'
      {mapped_values}
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
  GROUP BY
    client_id,
    sample_id
),
shutdown_ping AS (
  SELECT
    client_id AS client_id,
    sample_id AS sample_id,
    MIN(submission_timestamp) AS first_seen_timestamp,
    ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp ASC) AS all_dates,
    ARRAY_AGG(application.architecture RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS architecture,
    ARRAY_AGG(environment.build.build_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS app_build_id,
    ARRAY_AGG(normalized_app_name RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS app_name,
    ARRAY_AGG(environment.settings.locale RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS locale,
    ARRAY_AGG(application.platform_version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS platform_version,
    ARRAY_AGG(application.vendor RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS vendor,
    ARRAY_AGG(application.version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS app_version,
    ARRAY_AGG(application.xpcom_abi RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS xpcom_abi,
    ARRAY_AGG(document_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS document_id,
    ARRAY_AGG(environment.partner.distribution_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS distribution_id,
    ARRAY_AGG(environment.partner.distribution_version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS partner_distribution_version,
    ARRAY_AGG(environment.partner.distributor RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS partner_distributor,
    ARRAY_AGG(environment.partner.distributor_channel RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS partner_distributor_channel,
    ARRAY_AGG(environment.partner.partner_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS partner_id,
    ARRAY_AGG(
      environment.settings.attribution.campaign RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS attribution_campaign,
    ARRAY_AGG(environment.settings.attribution.content RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_content,
    ARRAY_AGG(
      environment.settings.attribution.experiment RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS attribution_experiment,
    ARRAY_AGG(environment.settings.attribution.medium RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_medium,
    ARRAY_AGG(environment.settings.attribution.source RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_source,
    ARRAY_AGG(environment.settings.attribution.ua RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_ua,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.load_path RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS engine_data_load_path,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.name RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS engine_data_name,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.origin RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS engine_data_origin,
    ARRAY_AGG(
      environment.settings.default_search_engine_data.submission_url RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS engine_data_submission_url,
    ARRAY_AGG(environment.system.apple_model_id RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS apple_model_id,
    ARRAY_AGG(metadata.geo.city RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS city,
    ARRAY_AGG(metadata.geo.db_version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS db_version,
    ARRAY_AGG(metadata.geo.subdivision1 RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS subdivision1,
    ARRAY_AGG(normalized_channel RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(normalized_country_code RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS country,
    ARRAY_AGG(normalized_os RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS normalized_os,
    ARRAY_AGG(normalized_os_version RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS normalized_os_version,
    ARRAY_AGG(
      payload.processes.parent.scalars.startup_profile_selection_reason RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS startup_profile_selection_reason,
    ARRAY_AGG(environment.settings.attribution.dltoken RESPECT NULLS ORDER BY submission_timestamp)[
      SAFE_OFFSET(0)
    ] AS attribution_dltoken,
    ARRAY_AGG(
      environment.settings.attribution.dlsource RESPECT NULLS
      ORDER BY
        submission_timestamp
    )[SAFE_OFFSET(0)] AS attribution_dlsource,
  FROM
    `moz-fx-data-shared-prod.telemetry.first_shutdown`
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= '2010-01-01'
      {mapped_values}
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
  GROUP BY
    client_id,
    sample_id
),
main_ping AS (
  -- The columns set as NULL are not available in clients_daily_v6 and need to be
  -- retrieved in the ETL from telemetry_stable.main_v4:<column>.
  SELECT
    client_id AS client_id,
    sample_id AS sample_id,
    -- The submission_timestamp_min is used to compare with the TIMESTAMP of
    -- the new_profile and first shutdown pings.
    -- It was implemented on Dec 16, 2019 and has data from 2018-10-30.
    IF(
      MIN(submission_date) >= '2018-10-30',
      MIN(submission_timestamp_min),
      TIMESTAMP(MIN(submission_date))
    ) AS first_seen_timestamp,
    ARRAY_AGG(DATE(submission_date) ORDER BY submission_date ASC) AS all_dates,
    CAST(
      NULL AS STRING
    ) AS architecture, -- main_v4:environment.build.architecture
    ARRAY_AGG(env_build_id RESPECT NULLS ORDER BY submission_date)[SAFE_OFFSET(0)] AS app_build_id,
    ARRAY_AGG(app_name RESPECT NULLS ORDER BY submission_date)[SAFE_OFFSET(0)] AS app_name,
    ARRAY_AGG(locale RESPECT NULLS ORDER BY submission_date)[SAFE_OFFSET(0)] AS locale,
    CAST(
      NULL AS STRING
    ) AS platform_version, -- main_v4:environment.build.platform_version
    ARRAY_AGG(vendor RESPECT NULLS ORDER BY submission_date)[SAFE_OFFSET(0)] AS vendor,
    ARRAY_AGG(app_version RESPECT NULLS ORDER BY submission_date)[SAFE_OFFSET(0)] AS app_version,
    CAST(
      NULL AS STRING
    ) AS xpcom_abi, -- main_v4:environment.build.xpcom_abi / application.xpcom_abi
    CAST(
      NULL AS STRING
    ) AS document_id, -- main_v4:document_id
    ARRAY_AGG(distribution_id RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS distribution_id,
    CAST(
      NULL AS STRING
    ) AS partner_distribution_version, -- main_v4:environment.partner.distribution_version
    CAST(
      NULL AS STRING
    ) AS partner_distributor, -- main_v4:environment.partner.distributor
    CAST(
      NULL AS STRING
    ) AS partner_distributor_channel, -- main_v4:environment.partner.distributor_channel
    CAST(
      NULL AS STRING
    ) AS partner_id, -- main_v4:environment.partner.distribution_id
    ARRAY_AGG(attribution.campaign RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS attribution_campaign,
    ARRAY_AGG(attribution.content RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS attribution_content,
    ARRAY_AGG(attribution.experiment RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS attribution_experiment,
    ARRAY_AGG(attribution.medium RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS attribution_medium,
    ARRAY_AGG(attribution.source RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS attribution_source,
    CAST(
      NULL AS STRING
    ) AS attribution_ua, -- main_v4:environment.settings.attribution.ua
    ARRAY_AGG(default_search_engine_data_load_path RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS engine_data_load_path,
    ARRAY_AGG(default_search_engine_data_name RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS engine_data_name,
    ARRAY_AGG(default_search_engine_data_origin RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS engine_data_origin,
    ARRAY_AGG(default_search_engine_data_submission_url RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS engine_data_submission_url,
    CAST(
      NULL AS STRING
    ) AS apple_model_id, -- main_v4:environment.system.apple_model_id
    ARRAY_AGG(city RESPECT NULLS ORDER BY submission_date)[SAFE_OFFSET(0)] AS city,
    CAST(
      NULL AS STRING
    ) AS db_version, -- main_v4:metadata.geo.db_version
    ARRAY_AGG(geo_subdivision1 RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS subdivision1,
    ARRAY_AGG(normalized_channel RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(country RESPECT NULLS ORDER BY submission_date)[SAFE_OFFSET(0)] AS country,
    ARRAY_AGG(os RESPECT NULLS ORDER BY submission_date)[SAFE_OFFSET(0)] AS normalized_os,
    ARRAY_AGG(normalized_os_version RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS normalized_os_version,
    CAST(
      NULL AS STRING
    ) AS startup_profile_selection_reason, -- main_v4:payload.processes.parent.scalars.startup_profile_selection_reason
    ARRAY_AGG(attribution.dltoken RESPECT NULLS ORDER BY submission_date)[
      SAFE_OFFSET(0)
    ] AS attribution_dltoken,
    CAST(
      NULL AS STRING
    ) AS attribution_dlsource -- main_v4:environment.settings.attribution.dlsource
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  WHERE
    {% if is_init() %}
      submission_date >= '2010-01-01'
      {mapped_values}
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    client_id,
    sample_id
),
-- The ping priority is required when different ping types have the exact same timestamp
unioned AS (
  SELECT
    *,
    'new_profile' AS source_ping,
    1 AS source_ping_priority
  FROM
    new_profile_ping
  UNION ALL
  SELECT
    *,
    'shutdown' AS source_ping,
    3 AS source_ping_priority
  FROM
    shutdown_ping
  UNION ALL
  SELECT
    *,
    'main' AS source_ping,
    2 AS source_ping_priority
  FROM
    main_ping
),
-- The next CTE unions all reported dates from all pings.
-- It's required to find the first and second seen dates.
dates_with_reporting_ping AS (
  SELECT
    client_id,
    ARRAY_CONCAT(
      ARRAY_AGG(
        STRUCT(
          all_dates AS value,
          unioned.source_ping AS value_source,
          all_dates AS value_date
        ) IGNORE NULLS
      )
    ) AS seen_dates
  FROM
    unioned
  LEFT JOIN
    UNNEST(all_dates) AS all_dates
  GROUP BY
    client_id
),
-- The next CTE returns the first_seen_date and reporting ping.
-- The ping type priority is used to prioritize which ping type to select when the timestamp is the same
-- The timestamp is retrieved to select the first_seen attributes.
first_seen_date AS (
  SELECT
    client_id,
    DATE(MIN(first_seen_timestamp)) AS first_seen_date,
    MIN(first_seen_timestamp) AS first_seen_timestamp,
    ARRAY_AGG(source_ping ORDER BY first_seen_timestamp, source_ping_priority)[
      SAFE_OFFSET(0)
    ] AS first_seen_source_ping
  FROM
    unioned
  GROUP BY
    client_id
),
-- The next CTE returns the second_seen_date. It finds the next date after first_seen_date
-- as reported by the main ping. Further dates reported by other pings are skipped.
-- The source ping for second_seen_date is always the main ping.
second_seen_date AS (
  SELECT
    client_id,
    IF(
      ARRAY_LENGTH(ARRAY_AGG(seen_dates)) > 1,
      ARRAY_AGG(seen_dates ORDER BY value_date ASC)[SAFE_OFFSET(1)],
      NULL
    ) AS second_seen_date
  FROM
    dates_with_reporting_ping
  LEFT JOIN
    UNNEST(seen_dates) AS seen_dates
  WHERE
    seen_dates.value_source = 'main'
  GROUP BY
    client_id
),
-- The next CTE returns the pings ever reported by each client
-- Different from other attributes, this data is updated daily when it's NULL,
-- so it's not limited to the first_seen_date.
reported_pings AS (
  SELECT
    client_id,
    'main' IN UNNEST(ARRAY_AGG(source_ping)) AS reported_main_ping,
    'new_profile' IN UNNEST(ARRAY_AGG(source_ping)) AS reported_new_profile_ping,
    'shutdown' IN UNNEST(ARRAY_AGG(source_ping)) AS reported_shutdown_ping
  FROM
    unioned
  GROUP BY
    client_id
),
_current AS (
  -- Get first value when the same ping type returns more than one record with the exact same TIMESTAMP
  SELECT
    unioned.client_id AS client_id,
    unioned.sample_id AS sample_id,
    fsd.first_seen_date AS first_seen_date,
    ssd.second_seen_date.value AS second_seen_date,
    unioned.* EXCEPT (
      client_id,
      sample_id,
      first_seen_timestamp,
      all_dates,
      source_ping,
      source_ping_priority
    ),
    STRUCT(
      fsd.first_seen_source_ping AS first_seen_date_source_ping,
      pings.reported_main_ping AS reported_main_ping,
      pings.reported_new_profile_ping AS reported_new_profile_ping,
      pings.reported_shutdown_ping AS reported_shutdown_ping
    ) AS metadata
  FROM
    unioned
  INNER JOIN
    first_seen_date AS fsd
  ON
    (
      unioned.client_id = fsd.client_id
      AND unioned.first_seen_timestamp = fsd.first_seen_timestamp
      AND unioned.source_ping = fsd.first_seen_source_ping
    )
  LEFT JOIN
    second_seen_date AS ssd
  ON
    unioned.client_id = ssd.client_id
  LEFT JOIN
    reported_pings AS pings
  ON
    unioned.client_id = pings.client_id
),
_previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2`
)
SELECT
{% if is_init() %}
    *
FROM
    _current
{% else %}
-- For the daily update:
-- The reported ping status in the metadata is updated when it's NULL.
-- The second_seen_date is updated when it's NULL and only if there is a
-- main ping reported on the submission_date.
-- Every other attribute remains as reported on the first_seen_date.
    IF(_previous.client_id IS NULL, _current, _previous).* REPLACE (
      IF(
        _previous.first_seen_date IS NOT NULL
        AND _previous.second_seen_date IS NULL
        AND _current.client_id IS NOT NULL
        AND _current.metadata.reported_main_ping,
        @submission_date,
        _previous.second_seen_date
      ) AS second_seen_date,
      (
        SELECT AS STRUCT
          IF(_previous.client_id IS NULL, _current, _previous).metadata.* REPLACE (
            IF(
              _previous.client_id IS NULL
              OR _previous.metadata.reported_main_ping IS FALSE
              AND _current.metadata.reported_main_ping IS TRUE,
              _current.metadata.reported_main_ping,
              _previous.metadata.reported_main_ping
            ) AS reported_main_ping,
            IF(
              _previous.client_id IS NULL
              OR _previous.metadata.reported_new_profile_ping IS FALSE
              AND _current.metadata.reported_new_profile_ping IS TRUE,
              _current.metadata.reported_new_profile_ping,
              _previous.metadata.reported_new_profile_ping
            ) AS reported_new_profile_ping,
            IF(
              _previous.client_id IS NULL
              OR _previous.metadata.reported_shutdown_ping IS FALSE
              AND _current.metadata.reported_shutdown_ping IS TRUE,
              _current.metadata.reported_shutdown_ping,
              _previous.metadata.reported_shutdown_ping
            ) AS reported_shutdown_ping
          )
      ) AS metadata
    )
FROM
  _previous
FULL JOIN
  _current
USING
    (client_id)
{% endif %}
