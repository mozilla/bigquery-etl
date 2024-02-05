WITH device_properties_at_session_start_event AS (
  --get all sessions starting on the submission date
  SELECT
    a.user_pseudo_id AS ga_client_id,
    CAST(e.value.int_value AS string) AS ga_session_id,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'ga_session_number'
      LIMIT
        1
    ).int_value AS ga_session_number,
    geo.country AS country,
    geo.region AS region,
    geo.city AS city,
    collected_traffic_source.manual_campaign_id AS campaign_id,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_term AS term,
    collected_traffic_source.manual_content AS content,
    collected_traffic_source.gclid AS gclid,
    device.category AS device_category,
    device.mobile_model_name AS mobile_device_model,
    device.mobile_marketing_name AS mobile_device_string,
    device.operating_system AS os,
    device.operating_system_version AS os_version,
    device.language AS `language`,
    device.web_info.browser AS browser,
    device.web_info.browser_version AS browser_version,
    PARSE_DATE('%Y%m%d', event_date) AS session_date,
    ROW_NUMBER() OVER (
      PARTITION BY
        a.user_pseudo_id,
        e.value.int_value
      ORDER BY
        a.event_timestamp ASC
    ) AS rnk
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*` a
  JOIN
    UNNEST(event_params) e
  WHERE
    _table_suffix = FORMAT_DATE('%Y%m%d', @submission_date)
    AND e.key = 'ga_session_id'
    AND e.value.int_value IS NOT NULL
    AND a.event_name = 'session_start'
  QUALIFY
    rnk = 1
),
--get all the details for that session from the session date and the next day
event_aggregates AS (
  SELECT
    a.user_pseudo_id AS ga_client_id,
    CAST(e.value.int_value AS string) AS ga_session_id,
    COUNTIF(event_name = 'page_view') AS pageviews,
    MIN(event_timestamp) AS min_event_timestamp,
    MAX(event_timestamp) AS max_event_timestamp,
    CAST(
      MAX(CASE WHEN event_name = 'product_download' THEN 1 ELSE 0 END) AS boolean
    ) AS had_download_event
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*` a
  JOIN
    UNNEST(event_params) e
  WHERE
    _table_suffix
    BETWEEN FORMAT_DATE('%Y%m%d', @submission_date)
    AND FORMAT_DATE('%Y%m%d', DATE_ADD(@submission_date, INTERVAL 1 DAY))
    AND e.key = 'ga_session_id'
    AND e.value.int_value IS NOT NULL
  GROUP BY
    a.user_pseudo_id,
    CAST(e.value.int_value AS string)
),
stub_session_ids_staging AS (
  SELECT
    user_pseudo_id AS ga_client_id,
    event_timestamp,
    CAST(
      (
        SELECT
          `value`
        FROM
          UNNEST(event_params)
        WHERE
          key = 'ga_session_id'
        LIMIT
          1
      ).int_value AS string
    ) AS ga_session_id,
    CAST(e.value.int_value AS string) AS stub_session_id
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  JOIN
    UNNEST(event_params) e
  WHERE
    _TABLE_SUFFIX
    BETWEEN FORMAT_DATE('%Y%m%d', @submission_date)
    AND FORMAT_DATE('%Y%m%d', DATE_ADD(@submission_date, INTERVAL 1 DAY))
    AND event_name = 'stub_session_set'
    AND e.key = 'id'
    AND e.value.int_value IS NOT NULL
),
last_stub_session_id AS (
  SELECT
    ga_client_id,
    event_timestamp,
    ga_session_id,
    stub_session_id,
    ROW_NUMBER() OVER (
      PARTITION BY
        ga_client_id,
        ga_session_id
      ORDER BY
        event_timestamp DESC
    ) AS stub_session_rnk
  FROM
    stub_session_ids_staging
  QUALIFY
    stub_session_rnk = 1
),
all_stub_session_ids AS (
  SELECT
    ga_client_id,
    ga_session_id,
    ARRAY_AGG(stub_session_id) AS all_reported_stub_session_ids
  FROM
    stub_session_ids_staging
  GROUP BY
    ga_client_id,
    ga_session_id
),
landing_page_by_session_staging AS (
  SELECT
    user_pseudo_id AS ga_client_id,
    CAST(
      (
        SELECT
          `value`
        FROM
          UNNEST(event_params)
        WHERE
          key = 'ga_session_id'
        LIMIT
          1
      ).int_value AS string
    ) AS ga_session_id,
    SPLIT(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1).string_value,
      '?'
    )[OFFSET(0)] AS page_location,
    event_timestamp
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*` a
  JOIN
    UNNEST(event_params) e
  WHERE
    _TABLE_SUFFIX
    BETWEEN FORMAT_DATE('%Y%m%d', @submission_date)
    AND FORMAT_DATE('%Y%m%d', DATE_ADD(@submission_date, INTERVAL 1 DAY))
    AND e.key = 'entrances'
    AND e.value.int_value = 1
),
landing_page_by_session AS (
  SELECT
    ga_client_id,
    ga_session_id,
    page_location,
    event_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY
        ga_client_id,
        ga_session_id
      ORDER BY
        event_timestamp ASC
    ) AS lp_rnk
  FROM
    landing_page_by_session_staging
  QUALIFY
    lp_rnk = 1
),
install_targets_staging AS (
  SELECT
    a.user_pseudo_id AS ga_client_id,
    CAST(e.value.int_value AS string) AS ga_session_id,
    event_timestamp,
    event_name AS install_event_name
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*` a
  JOIN
    UNNEST(event_params) e
  WHERE
    _table_suffix
    BETWEEN FORMAT_DATE('%Y%m%d', @submission_date)
    AND FORMAT_DATE('%Y%m%d', DATE_ADD(@submission_date, INTERVAL 1 DAY))
    AND e.key = 'ga_session_id'
    AND e.value.int_value IS NOT NULL
    AND a.event_name IN (
      'product_download'
    ) --using a list so when Stephanie creates more types, we can add here
),
last_install_target AS (
  SELECT
    ga_client_id,
    ga_session_id,
    event_timestamp,
    install_event_name,
    ROW_NUMBER() OVER (
      PARTITION BY
        ga_client_id,
        ga_session_id
      ORDER BY
        event_timestamp DESC
    ) AS install_tgt_rnk
  FROM
    install_targets_staging
  QUALIFY
    install_tgt_rnk = 1
),
all_install_targets AS (
  SELECT
    ga_client_id,
    ga_session_id,
    ARRAY_AGG(install_event_name) AS all_reported_install_targets
  FROM
    install_targets_staging
  GROUP BY
    ga_client_id,
    ga_session_id
)
SELECT
  a.ga_client_id,
  a.ga_session_id,
  a.session_date,
  CASE
    WHEN a.ga_session_number = 1
      THEN TRUE
    ELSE FALSE
  END AS is_first_session,
  a.ga_session_number AS session_number,
  b.max_event_timestamp - b.min_event_timestamp AS time_on_site,
  b.pageviews,
  a.country,
  a.region,
  a.city,
  a.campaign_id,
  a.campaign,
  a.source,
  a.medium,
  a.term,
  a.content,
  a.gclid,
  a.device_category,
  a.mobile_device_model,
  a.mobile_device_string,
  a.os,
  a.os_version,
  a.language,
  a.browser,
  a.browser_version,
  b.had_download_event,
  f.install_event_name AS last_reported_install_target,
  g.all_reported_install_targets,
  c.stub_session_id AS last_reported_stub_session_id,
  d.all_reported_stub_session_ids,
  e.page_location AS landing_screen
FROM
  device_properties_at_session_start_event a
LEFT JOIN
  event_aggregates b
  ON a.ga_client_id = b.ga_client_id
  AND a.ga_session_id = b.ga_session_id
LEFT JOIN
  last_stub_session_id c
  ON a.ga_client_id = c.ga_client_id
  AND a.ga_session_id = c.ga_session_id
LEFT JOIN
  all_stub_session_ids d
  ON a.ga_client_id = d.ga_client_id
  AND a.ga_session_id = d.ga_session_id
LEFT JOIN
  landing_page_by_session e
  ON a.ga_client_id = e.ga_client_id
  AND a.ga_session_id = e.ga_session_id
LEFT JOIN
  last_install_target f
  ON a.ga_client_id = f.ga_client_id
  AND a.ga_session_id = f.ga_session_id
LEFT JOIN
  all_install_targets g
  ON a.ga_client_id = g.ga_client_id
  AND a.ga_session_id = g.ga_session_id
