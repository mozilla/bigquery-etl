WITH rollouts_cte AS (
  SELECT
    normandy_slug AS rollout_slug
  FROM
    `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  WHERE
    "pocketNewtab" IN UNNEST(feature_ids)
    AND is_rollout
    AND IF(
      end_date IS NULL,
      @submission_date > start_date,
      @submission_date
      BETWEEN start_date
      AND end_date
    )
  UNION ALL
  -- all clients regardless of enrollment
  SELECT
    "" AS rollout_slug
),
source_table_cte_1 AS (
  SELECT
    metrics.uuid.legacy_telemetry_profile_group_id AS analysis_unit_id,
    ARRAY_AGG(DISTINCT rollout_slug) AS rollout_slugs,
    ARRAY_AGG(
      STRUCT(
        client_info.app_display_version AS app_display_version,
        app_version_major AS app_version_major,
        app_version_minor AS app_version_minor,
        app_version_patch AS app_version_patch,
        client_info.locale AS locale,
        normalized_channel AS normalized_channel,
        normalized_os AS normalized_os,
        normalized_os_version AS normalized_os_version,
        IF(
          normalized_country_code IN ('CA', 'DE', 'FR', 'GB', 'US'),
          normalized_country_code,
          'OTHER'
        ) AS normalized_country_code
      )
      ORDER BY
        submission_timestamp DESC
      LIMIT
        1
    )[OFFSET(0)] AS dimensions,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(
      ARRAY_CONCAT(
        COALESCE(ARRAY(SELECT key FROM UNNEST(ping_info.experiments)), []),
        -- all clients regardless of enrollment
        [""]
      )
    ) AS rollout_slug
  JOIN
    rollouts_cte
    USING (rollout_slug)
  WHERE
    CAST(submission_timestamp AS DATE) = @submission_date
  GROUP BY
    analysis_unit_id
),
source_table_cte_2 AS (
  SELECT
    metrics.uuid.legacy_telemetry_profile_group_id AS analysis_unit_id,
    LOGICAL_OR(metrics.boolean.pocket_is_signed_in) AS pocket_is_signed_in,
    LOGICAL_OR(metrics.boolean.pocket_enabled) AS pocket_enabled,
    LOGICAL_OR(
      metrics.boolean.pocket_sponsored_stories_enabled
    ) AS pocket_sponsored_stories_enabled,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "newtab" AND name = "opened")
    ) AS newtab_opened,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "pocket" AND name = "impression")
    ) AS pocket_impression,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "pocket" AND name = "click")
    ) AS pocket_click,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "pocket" AND name = "save")
    ) AS pocket_save,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "pocket" AND name = "dismiss")
    ) AS pocket_dismiss
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.newtab`
  WHERE
    CAST(submission_timestamp AS DATE) = @submission_date
  GROUP BY
    analysis_unit_id
),
all_dimensions_and_metrics_cte AS (
  SELECT
    analysis_unit_id,
    rollout_slug,
    (SELECT AS STRUCT source_table_cte_1.dimensions.*,) AS dimensions,
    pocket_is_signed_in,
    pocket_enabled,
    pocket_sponsored_stories_enabled,
    newtab_opened,
    pocket_impression,
    pocket_click,
    pocket_save,
    pocket_dismiss,
  FROM
    source_table_cte_1,
    UNNEST(rollout_slugs) AS rollout_slug
  JOIN
    source_table_cte_2
    USING (analysis_unit_id)
),
aggregates_cte AS (
  SELECT
    rollout_slug,
    dimensions,
    COUNT(*) AS count_analysis_unit_ids,
    COALESCE(
      CAST(COUNTIF(pocket_is_signed_in IS TRUE) AS FLOAT64),
      0
    ) AS pocket_is_signed_in_count_true,
    COALESCE(CAST(COUNTIF(pocket_enabled IS TRUE) AS FLOAT64), 0) AS pocket_enabled_count_true,
    COALESCE(
      CAST(COUNTIF(pocket_sponsored_stories_enabled IS TRUE) AS FLOAT64),
      0
    ) AS pocket_sponsored_stories_enabled_count_true,
    COALESCE(CAST(AVG(newtab_opened) AS FLOAT64), 0) AS newtab_opened_avg,
    COALESCE(CAST(AVG(pocket_impression) AS FLOAT64), 0) AS pocket_impression_avg,
    COALESCE(CAST(AVG(pocket_click) AS FLOAT64), 0) AS pocket_click_avg,
    COALESCE(CAST(AVG(pocket_save) AS FLOAT64), 0) AS pocket_save_avg,
    COALESCE(CAST(AVG(pocket_dismiss) AS FLOAT64), 0) AS pocket_dismiss_avg,
  FROM
    all_dimensions_and_metrics_cte
  GROUP BY
    rollout_slug,
    dimensions
),
ratios_cte AS (
  SELECT
    *,
  FROM
    aggregates_cte
)
SELECT
  @submission_date AS submission_date,
  rollout_slug,
  -- use json so that all features share a schema
  TO_JSON(dimensions) AS dimensions,
  count_analysis_unit_ids,
  metric,
  value,
FROM
  ratios_cte
-- unpivot to create consistent schema for wildcard table selects
  UNPIVOT (
    value
    FOR metric IN (
      pocket_is_signed_in_count_true,
      pocket_enabled_count_true,
      pocket_sponsored_stories_enabled_count_true,
      newtab_opened_avg,
      pocket_impression_avg,
      pocket_click_avg,
      pocket_save_avg,
      pocket_dismiss_avg
    )
  )
