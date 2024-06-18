CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.serp_categorization_unnested`
AS
WITH flat AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    document_id,
    CONCAT(document_id, '-', event_offset) AS event_id,
    event.timestamp AS event_timestamp,
    SAFE.TIMESTAMP_MILLIS(
      SAFE_CAST(mozfun.map.get_key(event.extra, 'glean_timestamp') AS INT64)
    ) AS glean_timestamp,
    mozfun.map.get_key(event.extra, 'channel') AS channel,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'app_version') AS INT) AS browser_major_version,
    mozfun.map.get_key(event.extra, 'region') AS country,
    mozfun.map.get_key(event.extra, 'provider') AS provider,
    mozfun.map.get_key(event.extra, 'partner_code') AS partner_code,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'tagged') AS BOOLEAN) AS tagged,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'is_shopping_page') AS BOOLEAN) AS is_shopping_page,
    SAFE_CAST(
      mozfun.map.get_key(event.extra, 'sponsored_category') AS INT
    ) AS sponsored_category_id,
    SAFE_CAST(
      mozfun.map.get_key(event.extra, 'sponsored_num_domains') AS INT
    ) AS sponsored_num_domains,
    SAFE_CAST(
      mozfun.map.get_key(event.extra, 'sponsored_num_unknown') AS INT
    ) AS sponsored_num_unknown,
    SAFE_CAST(
      mozfun.map.get_key(event.extra, 'sponsored_num_inconclusive') AS INT
    ) AS sponsored_num_inconclusive,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'organic_category') AS INT) AS organic_category_id,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'organic_num_domains') AS INT) AS organic_num_domains,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'organic_num_unknown') AS INT) AS organic_num_unknown,
    SAFE_CAST(
      mozfun.map.get_key(event.extra, 'organic_num_inconclusive') AS INT
    ) AS organic_num_inconclusive,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'num_ads_loaded') AS INT) AS num_ads_loaded,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'num_ads_visible') AS INT) AS num_ads_visible,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'num_ads_hidden') AS INT) AS num_ads_hidden,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'num_ads_clicked') AS INT) AS num_ads_clicked,
    mozfun.map.get_key(event.extra, 'mappings_version') AS mappings_version
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.serp_categorization_v1` AS s
  CROSS JOIN
    UNNEST(events) AS event
    WITH OFFSET AS event_offset
  WHERE
    event.category = 'serp'
    AND event.name = 'categorization'
)
SELECT
  *,
  nsp.category_name AS sponsored_category_name,
  norg.category_name AS organic_category_name
FROM
  flat AS f
LEFT JOIN
  `moz-fx-data-shared-prod.static.serp_category_name` AS nsp
  ON f.mappings_version = nsp.mappings_version
  AND f.sponsored_category_id = nsp.category_id
LEFT JOIN
  `moz-fx-data-shared-prod.static.serp_category_name` AS norg
  ON f.mappings_version = norg.mappings_version
  AND f.organic_category_id = norg.category_id
