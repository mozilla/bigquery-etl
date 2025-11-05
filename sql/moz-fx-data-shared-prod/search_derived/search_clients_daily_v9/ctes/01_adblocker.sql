WITH
-- list of ad blocking addons produced using this logic: https://github.com/mozilla/search-adhoc-analysis/tree/master/monetization-blocking-addons
adblocker_addons_cte AS (
  SELECT
    addon_id,
    addon_name
  FROM
    `moz-fx-data-shared-prod.revenue.monetization_blocking_addons`
  WHERE
    blocks_monetization
),
-- this table is the new glean adblocker addons metric ping (used to be legacy telemetry)
clients_with_adblocker_addons_cte AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    TRUE AS has_adblocker_addon
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`,
    UNNEST(JSON_QUERY_ARRAY(metrics.object.addons_active_addons)) AS addons
  INNER JOIN
    adblocker_addons_cte
    ON adblocker_addons_cte.addon_id = JSON_VALUE(addons, '$.id')
  WHERE
    DATE(submission_timestamp)
    BETWEEN '2025-06-25'
    AND '2025-09-25'
    AND sample_id = 0
    -- date(submission_timestamp) = @submission_date
    AND NOT BOOL(JSON_QUERY(addons, '$.userDisabled'))
    AND NOT BOOL(JSON_QUERY(addons, '$.appDisabled'))
    AND NOT BOOL(JSON_QUERY(addons, '$.blocklisted'))
  GROUP BY
    client_id,
    DATE(submission_timestamp)
)
SELECT
  *
FROM
  clients_with_adblocker_addons_cte
