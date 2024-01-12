CREATE TEMP FUNCTION isoEngine(engine STRING, s STRING, v INT64) AS (
  CASE
    WHEN engine = s
      THEN v
    ELSE 0
  END
);

CREATE TEMP FUNCTION mapSearches(engine STRING, v INT64) AS (
  STRUCT(
    isoEngine(engine, 'google', v) AS google,
    isoEngine(engine, 'bing', v) AS bing,
    isoEngine(engine, 'ddg', v) AS ddg,
    isoEngine(engine, 'yandex', v) AS yandex,
    isoEngine(engine, 'amazon', v) AS amazon,
    isoEngine(engine, 'other', v) AS other,
    v AS total
  )
);

WITH client_searches AS (
  SELECT
    client_id,
    mapSearches(engine, sap) AS sap,
    mapSearches(engine, tagged_sap) AS tagged_sap,
    mapSearches(engine, tagged_follow_on) AS tagged_follow_on,
    mapSearches(engine, organic) AS organic,
    mapSearches(engine, searches_with_ads) AS searches_with_ads,
    mapSearches(engine, ad_click) AS ad_clicks
  FROM
    (
      SELECT
        client_id,
        CASE
          WHEN STARTS_WITH(engine, 'google')
            THEN 'google'
          WHEN STARTS_WITH(engine, 'ddg')
            OR STARTS_WITH(engine, 'duckduckgo')
            THEN 'ddg'
          WHEN STARTS_WITH(engine, 'bing')
            THEN 'bing'
          WHEN STARTS_WITH(engine, 'yandex')
            THEN 'yandex'
          WHEN engine LIKE '%mazon%'
            THEN 'amazon'
          ELSE 'other'
        END AS engine,
        SUM(COALESCE(sap, 0)) AS sap,
        SUM(COALESCE(tagged_sap)) AS tagged_sap,
        SUM(COALESCE(tagged_follow_on)) AS tagged_follow_on,
        SUM(COALESCE(organic, 0)) AS organic,
        SUM(COALESCE(ad_click, 0)) AS ad_click,
        SUM(COALESCE(search_with_ads, 0)) AS searches_with_ads
      FROM
        `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
      WHERE
        submission_date = @submission_date
      GROUP BY
        1,
        2
    )
),
client_meta AS (
  SELECT
    submission_date,
    client_id,
    os,
    country,
    default_search_engine,
    subsession_hours_sum,
    scalar_parent_browser_engagement_tab_open_event_count_sum,
    scalar_parent_browser_engagement_total_uri_count_sum,
    devtools_toolbox_opened_count_sum,
    active_hours_sum,
    -- We apply this UDF on top of telemetry_derived.clients_last_seen_v1;
    -- it would be more convenient to use the telemetry.clients_last_seen view,
    -- but this saves on query complexity.
    mozfun.bits28.days_since_seen(days_seen_bits) AS days_since_seen,
    sync_configured,
    sap,
    tagged_sap,
    tagged_follow_on,
    organic,
    searches_with_ads,
    client_searches.ad_clicks,
    active_addons
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v1`
  LEFT JOIN
    client_searches
    USING (client_id)
  WHERE
    submission_date = @submission_date
),
addons_expanded AS (
  SELECT
    *,
    aa
  FROM
    client_meta
  CROSS JOIN
    UNNEST(active_addons) AS aa
),
daut AS (
  SELECT
    aa.addon_id,
    COUNT(DISTINCT client_id) AS dau
  FROM
    addons_expanded
  WHERE
    days_since_seen = 0
  GROUP BY
    1
),
waut AS (
  SELECT
    aa.addon_id,
    COUNT(DISTINCT client_id) AS wau
  FROM
    addons_expanded
  WHERE
    days_since_seen < 7
  GROUP BY
    1
),
maut AS (
  SELECT
    aa.addon_id,
    COUNT(DISTINCT client_id) AS mau
  FROM
    addons_expanded
  WHERE
    days_since_seen < 28
  GROUP BY
    1
),
names AS (
  SELECT
    addon_id,
    name,
    RANK() OVER (PARTITION BY addon_id ORDER BY n DESC) AS rank
  FROM
    (SELECT addon_id, name, COUNT(*) AS n FROM addons_expanded GROUP BY 1, 2)
),
default_search_engines AS (
  SELECT
    addon_id,
    default_search_engine AS most_common_default_search_engine,
    RANK() OVER (PARTITION BY addon_id ORDER BY n DESC) AS rank
  FROM
    (SELECT addon_id, default_search_engine, COUNT(*) AS n FROM addons_expanded GROUP BY 1, 2)
),
engagement AS (
  SELECT
    aa.addon_id,
    ANY_VALUE(aa.is_system) AS is_system,
    SUM(active_hours_sum) AS active_hours_sum,
    SUM(scalar_parent_browser_engagement_total_uri_count_sum) AS total_uri_count_sum,
    SUM(scalar_parent_browser_engagement_tab_open_event_count_sum) AS tab_open_event_count_sum,
    SUM(devtools_toolbox_opened_count_sum) AS devtools_toolbox_opened_count_sum,
    STRUCT(
      SUM(sap.google) AS google,
      SUM(sap.bing) AS bing,
      SUM(sap.ddg) AS ddg,
      SUM(sap.amazon) AS amazon,
      SUM(sap.yandex) AS yandex,
      SUM(sap.other) AS other,
      SUM(sap.total) AS total
    ) AS sap_searches,
    STRUCT(
      SUM(tagged_sap.google) AS google,
      SUM(tagged_sap.bing) AS bing,
      SUM(tagged_sap.ddg) AS ddg,
      SUM(tagged_sap.amazon) AS amazon,
      SUM(tagged_sap.yandex) AS yandex,
      SUM(tagged_sap.other) AS other,
      SUM(tagged_sap.total) AS total
    ) AS tagged_sap_searches,
    STRUCT(
      SUM(tagged_follow_on.google) AS google,
      SUM(tagged_follow_on.bing) AS bing,
      SUM(tagged_follow_on.ddg) AS ddg,
      SUM(tagged_follow_on.amazon) AS amazon,
      SUM(tagged_follow_on.yandex) AS yandex,
      SUM(tagged_follow_on.other) AS other,
      SUM(tagged_follow_on.total) AS total
    ) AS tagged_follow_on_searches,
    STRUCT(
      SUM(organic.google) AS google,
      SUM(organic.bing) AS bing,
      SUM(organic.ddg) AS ddg,
      SUM(organic.amazon) AS amazon,
      SUM(organic.yandex) AS yandex,
      SUM(organic.other) AS other,
      SUM(organic.total) AS total
    ) AS organic_searches,
    STRUCT(
      SUM(searches_with_ads.google) AS google,
      SUM(searches_with_ads.bing) AS bing,
      SUM(searches_with_ads.ddg) AS ddg,
      SUM(searches_with_ads.amazon) AS amazon,
      SUM(searches_with_ads.yandex) AS yandex,
      SUM(searches_with_ads.other) AS other,
      SUM(searches_with_ads.total) AS total
    ) AS searches_with_ads,
    STRUCT(
      SUM(ad_clicks.google) AS google,
      SUM(ad_clicks.bing) AS bing,
      SUM(ad_clicks.ddg) AS ddg,
      SUM(ad_clicks.amazon) AS amazon,
      SUM(ad_clicks.yandex) AS yandex,
      SUM(ad_clicks.other) AS other,
      SUM(ad_clicks.total) AS total
    ) AS ad_clicks
  FROM
    addons_expanded
  GROUP BY
    1
)
SELECT
  @submission_date AS submission_date,
  *
FROM
  (SELECT * EXCEPT (rank) FROM names WHERE rank = 1)
JOIN
  (SELECT * EXCEPT (rank) FROM default_search_engines WHERE rank = 1)
  USING (addon_id)
JOIN
  daut
  USING (addon_id)
JOIN
  waut
  USING (addon_id)
JOIN
  maut
  USING (addon_id)
JOIN
  engagement
  USING (addon_id)
