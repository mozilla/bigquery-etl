CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.legacy_ad_clicks_by_state`
AS
WITH pivoted AS (
  SELECT
    *
  FROM
    (
      SELECT
        `month`,
        normalized_engine,
        `state`,
        daily_ad_clicks
      FROM
        `moz-fx-data-shared-prod.search_derived.legacy_ad_clicks_by_state_v1`
    ) pivot(SUM(daily_ad_clicks) FOR normalized_engine IN ('Bing', 'DuckDuckGo', 'Google'))
  ORDER BY
    `month`,
    state
),
final AS (
  SELECT
    *
  FROM
    pivoted
)
SELECT
  *
FROM
  final
