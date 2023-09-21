-- Query for telemetry_derived.clients_first_seen_v2
{% if is_init() and parallel_run() %}
INSERT INTO
  {project_id}.{dataset_id}.{table_name}
{% endif %}
-- Ping subqueries use ANY_VALUE + HAVING MIN, which returns the earliest NOT NULL value.
-- It is used because the query runs faster than using GROUPING or ARRAYs.
-- A unitest is added to guarantee that this behaviour when NULL values are present.
WITH new_profile_ping AS (
  SELECT
    client_id AS client_id,
    sample_id AS sample_id,
    ANY_VALUE(submission_timestamp HAVING MIN submission_timestamp) AS first_seen_timestamp,
    ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp ASC) AS all_dates,
    ANY_VALUE(application.architecture HAVING MIN submission_timestamp) AS architecture,
    ANY_VALUE(environment.build.build_id HAVING MIN submission_timestamp) AS app_build_id,
    ANY_VALUE(normalized_app_name HAVING MIN submission_timestamp) AS app_name,
    ANY_VALUE(environment.settings.locale HAVING MIN submission_timestamp) AS locale,
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
      environment.settings.attribution.ua
      HAVING
        MIN submission_timestamp
    ) AS attribution_ua,
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
    ) AS attribution_dltoken,
    ANY_VALUE(
      environment.settings.attribution.dlsource
      HAVING
        MIN submission_timestamp
    ) AS attribution_dlsource
  FROM
    `moz-fx-data-shared-prod.telemetry.new_profile`
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= '2010-01-01'
      {% if parallel_run() %}
        AND sample_id = {sample_id}
      {% endif %}
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
    ANY_VALUE(submission_timestamp HAVING MIN submission_timestamp) AS first_seen_timestamp,
    ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp ASC) AS all_dates,
    ANY_VALUE(application.architecture HAVING MIN submission_timestamp) AS architecture,
    ANY_VALUE(environment.build.build_id HAVING MIN submission_timestamp) AS app_build_id,
    ANY_VALUE(normalized_app_name HAVING MIN submission_timestamp) AS app_name,
    ANY_VALUE(environment.settings.locale HAVING MIN submission_timestamp) AS locale,
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
      environment.settings.attribution.ua
      HAVING
        MIN submission_timestamp
    ) AS attribution_ua,
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
    ) AS attribution_dltoken,
    ANY_VALUE(
      environment.settings.attribution.dlsource
      HAVING
        MIN submission_timestamp
    ) AS attribution_dlsource
  FROM
    `moz-fx-data-shared-prod.telemetry.first_shutdown`
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= '2010-01-01'
      {% if parallel_run() %}
        AND sample_id = {sample_id}
      {% endif %}
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
      ANY_VALUE(submission_date HAVING MIN submission_date) >= '2018-10-30',
      MIN(submission_timestamp_min),
      TIMESTAMP(ANY_VALUE(submission_date HAVING MIN submission_date))
    ) AS first_seen_timestamp,
    ARRAY_AGG(DATE(submission_date) ORDER BY submission_date ASC) AS all_dates,
    CAST(NULL AS STRING) AS architecture, -- main_v4:environment.build.architecture
    ANY_VALUE(env_build_id HAVING MIN submission_date) AS app_build_id,
    ANY_VALUE(app_name HAVING MIN submission_date) AS app_name,
    ANY_VALUE(locale HAVING MIN submission_date) AS locale,
    CAST(NULL AS STRING) AS platform_version, -- main_v4:environment.build.platform_version
    ANY_VALUE(vendor HAVING MIN submission_date) AS vendor,
    ANY_VALUE(app_version HAVING MIN submission_date) AS app_version,
    CAST(NULL AS STRING) AS xpcom_abi, -- main_v4:environment.build.xpcom_abi / application.xpcom_abi
    CAST(NULL AS STRING) AS document_id, -- main_v4:document_id
    ANY_VALUE(distribution_id HAVING MIN submission_date) AS distribution_id,
    CAST(NULL AS STRING) AS partner_distribution_version, -- main_v4:environment.partner.distribution_version
    CAST(NULL AS STRING) AS partner_distributor, -- main_v4:environment.partner.distributor
    CAST(NULL AS STRING) AS partner_distributor_channel, -- main_v4:environment.partner.distributor_channel
    CAST(NULL AS STRING) AS partner_id, -- main_v4:environment.partner.distribution_id
    ANY_VALUE(attribution.campaign HAVING MIN submission_date) AS attribution_campaign,
    ANY_VALUE(attribution.content HAVING MIN submission_date) AS attribution_content,
    ANY_VALUE(attribution.experiment HAVING MIN submission_date) AS attribution_experiment,
    ANY_VALUE(attribution.medium HAVING MIN submission_date) AS attribution_medium,
    ANY_VALUE(attribution.source HAVING MIN submission_date) AS attribution_source,
    CAST(NULL AS STRING) AS attribution_ua, -- main_v4:environment.settings.attribution.ua
    ANY_VALUE(
      default_search_engine_data_load_path
      HAVING
        MIN submission_date
    ) AS engine_data_load_path,
    ANY_VALUE(default_search_engine_data_name HAVING MIN submission_date) AS engine_data_name,
    ANY_VALUE(default_search_engine_data_origin HAVING MIN submission_date) AS engine_data_origin,
    ANY_VALUE(
      default_search_engine_data_submission_url
      HAVING
        MIN submission_date
    ) AS engine_data_submission_url,
    CAST(NULL AS STRING) AS apple_model_id, -- main_v4:environment.system.apple_model_id
    ANY_VALUE(city HAVING MIN submission_date) AS city,
    CAST(NULL AS STRING) AS db_version, -- main_v4:metadata.geo.db_version
    ANY_VALUE(geo_subdivision1 HAVING MIN submission_date) AS subdivision1,
    ANY_VALUE(normalized_channel HAVING MIN submission_date) AS normalized_channel,
    ANY_VALUE(country HAVING MIN submission_date) AS country,
    ANY_VALUE(os HAVING MIN submission_date) AS normalized_os,
    ANY_VALUE(normalized_os_version HAVING MIN submission_date) AS normalized_os_version,
    CAST(NULL AS STRING) AS startup_profile_selection_reason, -- main_v4:payload.processes.parent.scalars.startup_profile_selection_reason
    ANY_VALUE(attribution.dltoken HAVING MIN submission_date) AS attribution_dltoken,
    CAST(NULL AS STRING) AS attribution_dlsource -- main_v4:environment.settings.attribution.dlsource
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  WHERE
    {% if is_init() %}
      submission_date >= '2010-01-01'
      {% if parallel_run() %}
        AND sample_id = {sample_id}
      {% endif %}
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    client_id,
    sample_id
),
unioned AS (
  SELECT
    *,
    'new_profile' AS source_ping
  FROM
    new_profile_ping
  UNION ALL
  SELECT
    *,
    'shutdown' AS source_ping
  FROM
    shutdown_ping
  UNION ALL
  SELECT
    *,
    'main' AS source_ping
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
-- The timestamp is also required to retrieve the first_seen attributes.
first_seen_date AS (
  SELECT
    client_id,
    DATE(MIN(first_seen_timestamp)) AS first_seen_date,
    MIN(first_seen_timestamp) AS first_seen_timestamp,
    ANY_VALUE(source_ping HAVING MIN first_seen_timestamp) AS first_seen_source_ping,
  FROM
    unioned
  GROUP BY
    client_id
),
-- The next CTE returns the second_seen_date. It finds the next date after first_seen_date
-- as reported by the main ping (if any). Further dates reported by other pings are skipped.
-- The source ping is not collected because it's always the main ping.
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
-- Different from other attributes, this data is not limited to the first_seen_date.
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
  SELECT
    unioned.client_id AS client_id,
    unioned.sample_id AS sample_id,
    fsd.first_seen_date AS first_seen_date,
    ssd.second_seen_date.value AS second_seen_date, --This date is always reported by the main ping
    unioned.* EXCEPT (client_id, sample_id, first_seen_timestamp, all_dates, source_ping),
    STRUCT(
      fsd.first_seen_source_ping AS first_seen_date_source_ping, -- All attributes are reported by this ping
      reported_main_ping,
      reported_new_profile_ping,
      reported_shutdown_ping
    ) AS metadata
  FROM
    unioned
  JOIN
    first_seen_date AS fsd
  USING
    (client_id, first_seen_timestamp)
  LEFT JOIN
    second_seen_date AS ssd
  USING
    (client_id)
  LEFT JOIN
    reported_pings AS pings
  USING
    (client_id)
),
_previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2`
)
SELECT
  {% if is_init() %}
    IF(_previous.client_id IS NULL, _current, _previous).*
  {% else %}
    -- For the daily update:
    -- The reported ping status in the metadata is always updated is NULL.
    -- The second_seen_date is updated if NULL and if there is a main ping reported,
    --  even if it's not the earliest timestamp of that day.
    -- Every other attribute remains as recorded on the first_seen_date.
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
  {% endif %}
FROM
  _previous
FULL JOIN
  _current
USING
  (client_id)
