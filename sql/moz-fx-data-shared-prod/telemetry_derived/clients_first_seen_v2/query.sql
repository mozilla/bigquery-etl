-- Query for telemetry_derived.clients_first_seen_v2
{% if is_init() and parallel_run() %}
INSERT INTO
  {project_id}.{dataset_id}.{table_name}
{% endif %}
WITH new_profile_ping AS (
  SELECT
    client_id AS client_id,
    sample_id AS sample_id,
    ANY_VALUE(submission_timestamp HAVING MIN submission_timestamp) AS first_seen_date,
    CAST(NULL AS TIMESTAMP) AS second_seen_date,
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
    ANY_VALUE(submission_timestamp HAVING MIN submission_timestamp) AS first_seen_date,
    CAST(NULL AS TIMESTAMP) AS second_seen_date,
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
  SELECT
    client_id AS client_id,
    sample_id AS sample_id,
    IF(
      ANY_VALUE(submission_date HAVING MIN submission_date) >= '2018-10-30',
      MIN(submission_timestamp_min),
      TIMESTAMP(ANY_VALUE(submission_date HAVING MIN submission_date))
    ) AS first_seen_date,
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
    ANY_VALUE(default_search_engine_data_name HAVING MIN submission_date) AS engine_data_name,
    ANY_VALUE(default_search_engine_data_origin HAVING MIN submission_date) AS engine_data_origin,
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
    ANY_VALUE(attribution.dltoken HAVING MIN submission_date) AS attribution_dltoken,
    CAST(NULL AS STRING) AS attribution_dlsource
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
-- This query unions the client's 'data from the 3 pings, resulting in a max. of 3 records per client_id (one per ping).
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
-- The next 3 subqueries are required to calculate the second seen date, addressing different scenarios:
-- - Calculate using 6 TIMESTAMPS: first and second TIMESTAMPS x 3 pings.
-- - Extract only the earliest TIMESTAMP per DATE for each ping, for cases when many pings are sent on the same DATE.
-- - Exclude new_profile ping duplicates (only one is expected), to avoid first and second seen date from this ping.
-- The next subquery returns a max. of 6 records per client_id: 3 pings x 2 earliest TIMESTAMPS per ping.
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
-- This subquery returns a max. of 2 rows per client_id with the first and second earliest TIMESTAMPs and reporting pings.
unioned_min_timestamp_per_date AS (
  SELECT
    client_id,
    value,
    MIN(value_date) AS value_date
  FROM
    unioned_with_all_dates
  LEFT JOIN
    UNNEST(seen_dates) AS seen_dates
  WHERE
    seen_dates.value IS NOT NULL
  GROUP BY
    client_id,
    value
),
-- This subquery returns one row per client_id with the second seen date and reporting ping.
unioned_second_dates AS (
  SELECT
    client_id,
    IF(
      ARRAY_LENGTH(ARRAY_AGG(seen_dates)) > 1,
      ARRAY_AGG(seen_dates ORDER BY value_date ASC)[SAFE_OFFSET(1)],
      NULL
    ) AS second_seen_details
  FROM
    unioned_with_all_dates
  LEFT JOIN
    UNNEST(seen_dates) AS seen_dates
  JOIN
    unioned_min_timestamp_per_date
  USING
    (client_id, value_date)
  GROUP BY
    client_id
),
-- This query returns one row per client_id with the earliest value reported each attribute.
-- For some attributes, the reporting ping is also recorded in the metadata.
_current AS (
  SELECT
    client_id,
    CAST(
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(STRUCT(CAST(sample_id AS STRING), ping_source, DATETIME(first_seen_date)))
      ).earliest_value AS INT
    ) AS sample_id,
    DATE(
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(
          STRUCT(CAST(DATE(first_seen_date) AS STRING), ping_source, DATETIME(first_seen_date))
        )
      ).earliest_date
    ) AS first_seen_date,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(architecture AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS architecture,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(app_build_id AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS app_build_id,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(app_name AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS app_name,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(platform_version AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS platform_version,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(vendor AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS vendor,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(app_version AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS app_version,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(xpcom_abi AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS xpcom_abi,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(document_id AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS document_id,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(distribution_id AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS distribution_id,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(
        STRUCT(CAST(partner_distribution_version AS STRING), ping_source, DATETIME(first_seen_date))
      )
    ).earliest_value AS partner_distribution_version,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(partner_distributor AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS partner_distributor,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(
        STRUCT(CAST(partner_distributor_channel AS STRING), ping_source, DATETIME(first_seen_date))
      )
    ).earliest_value AS partner_distributor_channel,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(partner_id AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS partner_id,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(
        STRUCT(CAST(engine_data_load_path AS STRING), ping_source, DATETIME(first_seen_date))
      )
    ).earliest_value AS engine_data_load_path,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(engine_data_name AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS engine_data_name,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(engine_data_origin AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS engine_data_origin,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(
        STRUCT(CAST(engine_data_submission_url AS STRING), ping_source, DATETIME(first_seen_date))
      )
    ).earliest_value AS engine_data_submission_url,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(apple_model_id AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS apple_model_id,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(city AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS city,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(subdivision1 AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS subdivision1,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(normalized_channel AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS normalized_channel,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(country AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS country,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(normalized_os AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS normalized_os,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(
        STRUCT(CAST(normalized_os_version AS STRING), ping_source, DATETIME(first_seen_date))
      )
    ).earliest_value AS normalized_os_version,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(
        STRUCT(
          CAST(startup_profile_selection_reason AS STRING),
          ping_source,
          DATETIME(first_seen_date)
        )
      )
    ).earliest_value AS startup_profile_selection_reason,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(attribution_dltoken AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS attribution_dltoken,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(attribution_dlsource AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS attribution_dlsource,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(
        STRUCT(CAST(attribution_campaign AS STRING), ping_source, DATETIME(first_seen_date))
      )
    ).earliest_value AS attribution_campaign,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(attribution_content AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS attribution_content,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(
        STRUCT(CAST(attribution_experiment AS STRING), ping_source, DATETIME(first_seen_date))
      )
    ).earliest_value AS attribution_experiment,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(attribution_medium AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS attribution_medium,
    mozfun.norm.get_earliest_value(
      ARRAY_AGG(STRUCT(CAST(attribution_source AS STRING), ping_source, DATETIME(first_seen_date)))
    ).earliest_value AS attribution_source,
    STRUCT(
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(STRUCT(CAST(first_seen_date AS STRING), ping_source, DATETIME(first_seen_date)))
      ).earliest_value_source AS first_seen_date__source_ping,
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(
          STRUCT(CAST(attribution_campaign AS STRING), ping_source, DATETIME(first_seen_date))
        )
      ).earliest_value_source AS attribution_campaign__source_ping,
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(
          STRUCT(CAST(attribution_content AS STRING), ping_source, DATETIME(first_seen_date))
        )
      ).earliest_value_source AS attribution_content__source_ping,
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(
          STRUCT(CAST(attribution_experiment AS STRING), ping_source, DATETIME(first_seen_date))
        )
      ).earliest_value_source AS attribution_experiment__source_ping,
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(
          STRUCT(CAST(attribution_medium AS STRING), ping_source, DATETIME(first_seen_date))
        )
      ).earliest_value_source AS attribution_medium__source_ping,
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(
          STRUCT(CAST(attribution_source AS STRING), ping_source, DATETIME(first_seen_date))
        )
      ).earliest_value_source AS attribution_source__source_ping,
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(STRUCT(CAST(attribution_dltoken AS STRING), ping_source, DATETIME(first_seen_date)))
      ).earliest_value_source AS attribution_dltoken__source_ping,
      mozfun.norm.get_earliest_value(
        ARRAY_AGG(STRUCT(CAST(attribution_dlsource AS STRING), ping_source, DATETIME(first_seen_date)))
      ).earliest_value_source AS attribution_dlsource__source_ping,
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
-- This query returns one row per client_id including the core attributes unioned with the second_seen date.
_current_with_second_date AS (
  SELECT
    _current.client_id,
    _current.sample_id,
    _current.first_seen_date,
    DATE(second_seen_details.value) AS second_seen_date,
    _current.* EXCEPT (client_id, sample_id, first_seen_date, metadata),
    STRUCT(
      metadata.first_seen_date__source_ping,
      second_seen_details.value_source AS second_seen_date__source_ping,
      metadata.attribution_campaign__source_ping,
      metadata.attribution_content__source_ping,
      metadata.attribution_experiment__source_ping,
      metadata.attribution_medium__source_ping,
      metadata.attribution_source__source_ping,
      metadata.attribution_dltoken__source_ping,
      metadata.attribution_dlsource__source_ping,
      metadata.reported_main_ping,
      metadata.reported_new_profile_ping,
      metadata.reported_shutdown_ping
    ) AS metadata
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
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2`
)
SELECT
  {% if is_init() %}
    IF(_previous.client_id IS NULL, _current, _previous).*
  {% else %}
    IF(_previous.client_id IS NULL, _current, _previous).* REPLACE (
      IF(
        _previous.first_seen_date IS NOT NULL
        AND _previous.second_seen_date IS NULL
        AND _current.client_id IS NOT NULL,
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
  _current_with_second_date _current
USING
  (client_id)
