WITH raw_serp_events AS (
  SELECT
    *,
    DATE(submission_timestamp) AS submission_date,
    mozfun.map.get_key(event.extra, 'impression_id') AS impression_id,
    event
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`,
    UNNEST(events) AS event
  WHERE
    event.category = 'serp'
    AND DATE(submission_timestamp) >= '2023-09-01'
    AND TIMESTAMP_DIFF(submission_timestamp, timestamp '2023-09-01', DAY) IN (0, 1)
  -- output limiting for testing
    AND sample_id = 3
    AND normalized_channel = 'release'
),
serp_event_counts AS (
  SELECT
    impression_id,
    COUNTIF(event.name = 'impression') AS n_impressions,
    COUNTIF(event.name = 'engagement') AS n_engagements,
    COUNTIF(event.name = 'abandonment') AS n_abandonments,
  FROM
    raw_serp_events
  GROUP BY
    impression_id
),
filtered_impression_ids AS (
  -- select serp sessions/impression IDs with the expected combinations of events
  SELECT
    impression_id
  FROM
    serp_event_counts
  WHERE
    n_impressions = 1
    AND ((n_engagements >= 1 AND n_abandonments = 0) OR (n_engagements = 0 AND n_abandonments = 1))
),
serp_events AS (
  SELECT
    *
  FROM
    raw_serp_events
  INNER JOIN
    filtered_impression_ids
  USING
    (impression_id)
),
impressions AS (
  -- pull top-level fields from the impression event
  SELECT
    impression_id,
    submission_date,
    client_info.client_id AS glean_client_id,
    metrics.uuid.legacy_telemetry_client_id AS legacy_telemetry_client_id,
    event.timestamp AS event_timestamp,
    normalized_channel,
    normalized_country_code,
    client_info.os,
    mozfun.norm.browser_version_info(client_info.app_display_version) AS browser_version_info,
    sample_id,
    ping_info.experiments,
    CAST(mozfun.map.get_key(event.extra, 'is_shopping_page') AS bool) AS is_shopping_page,
    mozfun.map.get_key(event.extra, 'provider') AS search_engine,
    mozfun.map.get_key(event.extra, 'source') AS sap_source,
    CAST(mozfun.map.get_key(event.extra, 'tagged') AS bool) AS is_tagged
  FROM
    serp_events
  WHERE
    event.name = 'impression'
    AND submission_date = '2023-09-01'
),
abandonments AS (
  SELECT
    impression_id,
    mozfun.map.get_key(event.extra, 'reason') AS abandon_reason
  FROM
    serp_events
  WHERE
    event.name = 'abandonment'
),
engagement_counts AS (
  SELECT
    impression_id,
    COUNTIF(action = 'clicked' AND target = 'ad_carousel') AS num_ad_carousel_clicks,
    COUNTIF(action = 'clicked' AND target = 'ad_link') AS num_ad_link_clicks,
    COUNTIF(action = 'clicked' AND target = 'ad_sitelink') AS num_ad_sitelink_clicks,
    COUNTIF(action = 'clicked' AND target = 'ad_sidebar') AS num_ad_sidebar_clicks,
    COUNTIF(
      action = 'clicked'
      AND target = 'refined_search_buttons'
    ) AS num_refined_search_buttons_clicks,
    COUNTIF(action = 'clicked' AND target = 'shopping_tab') AS num_shopping_tab_clicks,
    COUNTIF(action = 'clicked' AND target = 'non_ads_link') AS num_non_ads_link_clicks,
    COUNTIF(
      action = 'clicked'
      AND target = 'incontent_searchbox'
    ) AS num_incontent_searchbox_clicks,
    COUNTIF(action = 'expanded' AND target = 'ad_carousel') AS num_ad_carousel_expansions,
    COUNTIF(
      action = 'expanded'
      AND target = 'refined_search_buttons'
    ) AS num_refined_search_buttons_expansions,
    COUNTIF(
      action = 'submitted'
      AND target = 'incontent_searchbox'
    ) AS num_incontent_searchbox_submits,
    COUNT(*) AS num_engagements
  FROM
    (
      SELECT
        impression_id,
        mozfun.map.get_key(event.extra, 'action') AS action,
        mozfun.map.get_key(event.extra, 'target') AS target,
      FROM
        serp_events
      WHERE
        event.name = 'engagement'
    )
  GROUP BY
    impression_id
),
ad_impression_counts AS (
  SELECT
    impression_id,
    SUM(
      CASE
        WHEN component = 'ad_carousel'
          THEN ads_loaded
        ELSE 0
      END
    ) AS num_ad_carousel_ads_loaded,
    SUM(
      CASE
        WHEN component = 'ad_carousel'
          THEN ads_visible
        ELSE 0
      END
    ) AS num_ad_carousel_ads_visible,
    SUM(
      CASE
        WHEN component = 'ad_carousel'
          THEN ads_hidden
        ELSE 0
      END
    ) AS num_ad_carousel_ads_hidden,
    SUM(CASE WHEN component = 'ad_link' THEN ads_loaded ELSE 0 END) AS num_ad_link_ads_loaded,
    SUM(CASE WHEN component = 'ad_link' THEN ads_visible ELSE 0 END) AS num_ad_link_ads_visible,
    SUM(CASE WHEN component = 'ad_link' THEN ads_hidden ELSE 0 END) AS num_ad_link_ads_hidden,
    SUM(CASE WHEN component = 'ad_sidebar' THEN ads_loaded ELSE 0 END) AS num_ad_sidebar_ads_loaded,
    SUM(
      CASE
        WHEN component = 'ad_sidebar'
          THEN ads_visible
        ELSE 0
      END
    ) AS num_ad_sidebar_ads_visible,
    SUM(CASE WHEN component = 'ad_sidebar' THEN ads_hidden ELSE 0 END) AS num_ad_sidebar_ads_hidden,
    SUM(
      CASE
        WHEN component = 'ad_sitelink'
          THEN ads_loaded
        ELSE 0
      END
    ) AS num_ad_sitelink_ads_loaded,
    SUM(
      CASE
        WHEN component = 'ad_sitelink'
          THEN ads_visible
        ELSE 0
      END
    ) AS num_ad_sitelink_ads_visible,
    SUM(
      CASE
        WHEN component = 'ad_sitelink'
          THEN ads_hidden
        ELSE 0
      END
    ) AS num_ad_sitelink_ads_hidden,
    SUM(
      CASE
        WHEN component = 'refined_search_buttons'
          THEN ads_loaded
        ELSE 0
      END
    ) AS num_refined_search_buttons_ads_loaded,
    SUM(
      CASE
        WHEN component = 'refined_search_buttons'
          THEN ads_visible
        ELSE 0
      END
    ) AS num_refined_search_buttons_ads_visible,
    SUM(
      CASE
        WHEN component = 'refined_search_buttons'
          THEN ads_hidden
        ELSE 0
      END
    ) AS num_refined_search_buttons_ads_hidden,
    SUM(
      CASE
        WHEN component = 'shopping_tab'
          THEN ads_loaded
        ELSE 0
      END
    ) AS num_shopping_tab_ads_loaded,
    SUM(
      CASE
        WHEN component = 'shopping_tab'
          THEN ads_visible
        ELSE 0
      END
    ) AS num_shopping_tab_ads_visible,
    SUM(
      CASE
        WHEN component = 'shopping_tab'
          THEN ads_hidden
        ELSE 0
      END
    ) AS num_shopping_tab_ads_hidden,
    SUM(ads_loaded) AS num_ads_loaded
  FROM
    (
      SELECT
        impression_id,
        mozfun.map.get_key(event.extra, 'component') AS component,
        CAST(mozfun.map.get_key(event.extra, 'ads_loaded') AS int) AS ads_loaded,
        CAST(mozfun.map.get_key(event.extra, 'ads_visible') AS int) AS ads_visible,
        CAST(mozfun.map.get_key(event.extra, 'ads_hidden') AS int) AS ads_hidden,
      FROM
        serp_events
      WHERE
        event.name = 'ad_impression'
    )
  GROUP BY
    impression_id
),
serp_summary AS (
  SELECT
    impression_id,
    submission_date,
    glean_client_id,
    legacy_telemetry_client_id,
    event_timestamp,
    normalized_channel,
    normalized_country_code,
    os,
    browser_version_info,
    sample_id,
    experiments,
    is_shopping_page,
    search_engine,
    sap_source,
    is_tagged,
    COALESCE(num_engagements, 0) > 0 AS is_engaged,
    COALESCE(num_ads_loaded, 0) > 0 AS ads_loaded,
    -- engagement counts
    COALESCE(num_ad_carousel_clicks, 0) AS num_ad_carousel_clicks,
    COALESCE(num_ad_link_clicks, 0) AS num_ad_link_clicks,
    COALESCE(num_ad_sitelink_clicks, 0) AS num_ad_sitelink_clicks,
    COALESCE(num_ad_sidebar_clicks, 0) AS num_ad_sidebar_clicks,
    COALESCE(num_refined_search_buttons_clicks, 0) AS num_refined_search_buttons_clicks,
    COALESCE(num_shopping_tab_clicks, 0) AS num_shopping_tab_clicks,
    COALESCE(num_non_ads_link_clicks, 0) AS num_non_ads_link_clicks,
    COALESCE(num_incontent_searchbox_clicks, 0) AS num_incontent_searchbox_clicks,
    COALESCE(num_ad_carousel_expansions, 0) AS num_ad_carousel_expansions,
    COALESCE(num_refined_search_buttons_expansions, 0) AS num_refined_search_buttons_expansions,
    COALESCE(num_incontent_searchbox_submits, 0) AS num_incontent_searchbox_submits,
    -- abandonment reason
    abandon_reason,
    -- ad impression counts
    -- for each component, ads_visible + ads_blocked + ads_notshowing = ads_loaded
    COALESCE(num_ad_carousel_ads_visible, 0) AS num_ad_carousel_ads_visible,
    COALESCE(num_ad_carousel_ads_hidden, 0) AS num_ad_carousel_ads_blocked,
    COALESCE(
      num_ad_carousel_ads_loaded - num_ad_carousel_ads_visible - num_ad_carousel_ads_hidden,
      0
    ) AS num_ad_carousel_ads_notshowing,
    COALESCE(num_ad_link_ads_visible, 0) AS num_ad_link_ads_visible,
    COALESCE(num_ad_link_ads_hidden, 0) AS num_ad_link_ads_blocked,
    COALESCE(
      num_ad_link_ads_loaded - num_ad_link_ads_visible - num_ad_link_ads_hidden,
      0
    ) AS num_ad_link_ads_notshowing,
    COALESCE(num_ad_sidebar_ads_visible, 0) AS num_ad_sidebar_ads_visible,
    COALESCE(num_ad_sidebar_ads_hidden, 0) AS num_ad_sidebar_ads_blocked,
    COALESCE(
      num_ad_sidebar_ads_loaded - num_ad_sidebar_ads_visible - num_ad_sidebar_ads_hidden,
      0
    ) AS num_ad_sidebar_ads_notshowing,
    COALESCE(num_ad_sitelink_ads_visible, 0) AS num_ad_sitelink_ads_visible,
    COALESCE(num_ad_sitelink_ads_hidden, 0) AS num_ad_sitelink_ads_blocked,
    COALESCE(
      num_ad_sitelink_ads_loaded - num_ad_sitelink_ads_visible - num_ad_sitelink_ads_hidden,
      0
    ) AS num_ad_sitelink_ads_notshowing,
    COALESCE(num_refined_search_buttons_ads_visible, 0) AS num_refined_search_buttons_ads_visible,
    COALESCE(num_refined_search_buttons_ads_hidden, 0) AS num_refined_search_buttons_ads_blocked,
    COALESCE(
      num_refined_search_buttons_ads_loaded - num_refined_search_buttons_ads_visible - num_refined_search_buttons_ads_hidden,
      0
    ) AS num_ad_link_ads_notshowing,
    COALESCE(num_shopping_tab_ads_visible, 0) AS num_shopping_tab_ads_visible,
    COALESCE(num_shopping_tab_ads_hidden, 0) AS num_shopping_tab_ads_blocked,
    COALESCE(
      num_shopping_tab_ads_loaded - num_shopping_tab_ads_visible - num_shopping_tab_ads_hidden,
      0
    ) AS num_ad_link_ads_notshowing,
  FROM
    impressions
  LEFT JOIN
    engagement_counts
  USING
    (impression_id)
  LEFT JOIN
    abandonments
  USING
    (impression_id)
  LEFT JOIN
    ad_impression_counts
  USING
    (impression_id)
)
SELECT
  * EXCEPT (experiments)
FROM
  serp_summary
-- order by impression_id
LIMIT
  100
