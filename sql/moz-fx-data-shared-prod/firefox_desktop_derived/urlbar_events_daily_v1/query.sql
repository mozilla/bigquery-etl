WITH temp_unnested AS (
  SELECT
    submission_date,
    event_id,
    event_action,
    res.product_result_type AS product_result_type,
    normalized_channel,
    normalized_country_code,
    normalized_os,
    os_version,
    app_version,
    pref_ohttp_available,
    pref_ohttp_enabled,
    IF(pref_ohttp_available IS TRUE AND pref_ohttp_enabled IS TRUE, TRUE, FALSE) AS ohttp_enabled,
    normalized_engine,
    sap,
    pref_fx_suggestions AS firefox_suggest_enabled,
    pref_sponsored_suggestions AS sponsored_suggestions_enabled,
    IF(
      res.result_type LIKE '%\\_sponsored%'
      OR res.result_type LIKE '%\\_yelp%'
      OR res.result_type IN ('weather'),
      TRUE,
      FALSE
    ) AS is_sponsored,
    IF(res.result_type LIKE '%\\_adaptive%', TRUE, FALSE) AS is_adaptive,
    IF(res.result_type LIKE '%\\_semantic%', TRUE, FALSE) AS is_semantic,
    IF(res.result_type LIKE '%\\_serp%', TRUE, FALSE) AS is_serp,
    IF(res.result_type LIKE 'merino\\_%', TRUE, FALSE) AS is_online_suggest,
    IF(res.result_type LIKE 'rust\\_%', TRUE, FALSE) AS is_offline_suggest,
    IF(
      res.result_type LIKE '%\\_yelp%'
      OR res.result_type IN ('weather'),
      TRUE,
      FALSE
    ) AS is_geo_local,
    IF(
      res.result_type NOT IN (
        'remote_tab',
        'search_suggest',
        'search_suggest_rich',
        'trending_search',
        'trending_search_rich',
        'weather'
      )
      OR res.result_type LIKE 'merino\\_%',
      TRUE,
      FALSE
    ) AS is_from_device,
    IF(res.result_group = 'top_pick', TRUE, FALSE) AS is_top_pick,
    is_terminal,
    (
      product_selected_result = res.product_result_type
      AND event_action = 'engaged'
      AND is_terminal
    ) AS is_clicked,
    (product_selected_result = res.product_result_type AND event_action = 'annoyance') AS is_annoyed
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.urlbar_events_v2`
  CROSS JOIN
    UNNEST(results) AS res
  WHERE
    submission_date = @submission_date
),
temp_session AS (
  SELECT
    submission_date,
    event_id,
    product_result_type,
    is_terminal,
    ANY_VALUE(normalized_channel) AS normalized_channel,
    ANY_VALUE(normalized_country_code) AS normalized_country_code,
    ANY_VALUE(normalized_os) AS normalized_os,
    ANY_VALUE(os_version) AS os_version,
    ANY_VALUE(app_version) AS app_version,
    ANY_VALUE(pref_ohttp_available) AS pref_ohttp_available,
    ANY_VALUE(pref_ohttp_enabled) AS pref_ohttp_enabled,
    ANY_VALUE(ohttp_enabled) AS ohttp_enabled,
    ANY_VALUE(normalized_engine) AS normalized_engine,
    ANY_VALUE(sap) AS sap,
    ANY_VALUE(firefox_suggest_enabled) AS firefox_suggest_enabled,
    ANY_VALUE(sponsored_suggestions_enabled) AS sponsored_suggestions_enabled,
    LOGICAL_OR(is_sponsored) AS is_sponsored,
    LOGICAL_OR(is_adaptive) AS is_adaptive,
    LOGICAL_OR(is_semantic) AS is_semantic,
    LOGICAL_OR(is_serp) AS is_serp,
    LOGICAL_OR(is_online_suggest) AS is_online_suggest,
    LOGICAL_OR(is_offline_suggest) AS is_offline_suggest,
    LOGICAL_OR(is_geo_local) AS is_geo_local,
    LOGICAL_OR(is_from_device) AS is_from_device,
    LOGICAL_OR(is_top_pick) AS is_top_pick,
    LOGICAL_OR(is_clicked) AS is_clicked,
    LOGICAL_OR(is_annoyed) AS is_annoyed,
    LOGICAL_OR(is_terminal = TRUE) AS is_impression
  FROM
    temp_unnested
  GROUP BY
    submission_date,
    event_id,
    product_result_type,
    is_terminal
),
total_urlbar_sessions AS (
  SELECT
    submission_date,
    normalized_country_code,
    normalized_channel,
    normalized_os,
    os_version,
    app_version,
    pref_ohttp_available,
    pref_ohttp_enabled,
    ohttp_enabled,
    normalized_engine,
    sap,
    is_sponsored,
    is_adaptive,
    is_online_suggest,
    is_semantic,
    is_serp,
    is_offline_suggest,
    is_geo_local,
    is_from_device,
    is_top_pick,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled,
    COUNT(DISTINCT event_id) AS urlbar_sessions
  FROM
    temp_session
  WHERE
    is_terminal = TRUE
  GROUP BY
    submission_date,
    normalized_country_code,
    normalized_channel,
    normalized_os,
    os_version,
    app_version,
    pref_ohttp_available,
    pref_ohttp_enabled,
    ohttp_enabled,
    normalized_engine,
    sap,
    is_sponsored,
    is_adaptive,
    is_online_suggest,
    is_semantic,
    is_serp,
    is_offline_suggest,
    is_geo_local,
    is_from_device,
    is_top_pick,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled
),
daily_counts AS (
  SELECT
    submission_date,
    normalized_country_code,
    normalized_channel,
    normalized_os,
    os_version,
    app_version,
    pref_ohttp_available,
    pref_ohttp_enabled,
    ohttp_enabled,
    normalized_engine,
    sap,
    is_sponsored,
    is_adaptive,
    is_online_suggest,
    is_semantic,
    is_serp,
    is_offline_suggest,
    is_geo_local,
    is_from_device,
    is_top_pick,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled,
    product_result_type,
    COUNTIF(is_impression) AS urlbar_impressions,
    COUNTIF(is_clicked) AS urlbar_clicks,
    COUNTIF(is_annoyed) AS urlbar_annoyances,
  FROM
    temp_session
  GROUP BY
    submission_date,
    normalized_country_code,
    normalized_channel,
    normalized_os,
    os_version,
    app_version,
    pref_ohttp_available,
    pref_ohttp_enabled,
    ohttp_enabled,
    normalized_engine,
    sap,
    is_sponsored,
    is_adaptive,
    is_online_suggest,
    is_semantic,
    is_serp,
    is_offline_suggest,
    is_geo_local,
    is_from_device,
    is_top_pick,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled,
    product_result_type
),
join_counts_sessions AS (
  SELECT
    submission_date,
    normalized_country_code,
    normalized_channel,
    normalized_os,
    os_version,
    daily_counts.app_version,
    pref_ohttp_available,
    pref_ohttp_enabled,
    ohttp_enabled,
    normalized_engine,
    sap,
    is_sponsored,
    is_adaptive,
    is_online_suggest,
    is_semantic,
    is_serp,
    is_offline_suggest,
    is_geo_local,
    is_from_device,
    is_top_pick,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled,
    product_result_type,
    urlbar_impressions,
    urlbar_clicks,
    urlbar_annoyances,
    urlbar_sessions
  FROM
    daily_counts
  LEFT JOIN
    total_urlbar_sessions
    USING (
      submission_date,
      normalized_country_code,
      normalized_channel,
      normalized_os,
      os_version,
      pref_ohttp_available,
      pref_ohttp_enabled,
      ohttp_enabled,
      normalized_engine,
      sap,
      is_sponsored,
      is_adaptive,
      is_online_suggest,
      is_semantic,
      is_serp,
      is_offline_suggest,
      is_geo_local,
      is_from_device,
      is_top_pick,
      firefox_suggest_enabled,
      sponsored_suggestions_enabled
    )
),
final AS (
  SELECT
    *
  FROM
    join_counts_sessions
)
SELECT
  *
FROM
  final
