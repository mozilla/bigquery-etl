WITH rollouts_cte AS (
  SELECT
    normandy_slug AS rollout_slug
  FROM
    `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  WHERE
    "newtabSponsoredContent" IN UNNEST(feature_ids)
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
    LOGICAL_OR(metrics.boolean.topsites_enabled) AS topsites_enabled,
    LOGICAL_OR(metrics.boolean.topsites_sponsored_enabled) AS topsites_sponsored_enabled,
    SUM(metrics.quantity.topsites_rows) AS topsites_rows,
    SUM(
      metrics.quantity.topsites_sponsored_tiles_configured
    ) AS topsites_sponsored_tiles_configured,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "pocket" AND name = "click")
    ) AS pocket_click,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "pocket" AND name = "impression")
    ) AS pocket_impression,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "pocket" AND name = "dismiss")
    ) AS pocket_dismiss,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "pocket" AND name = "save")
    ) AS pocket_save,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "topsites" AND name = "impression")
    ) AS topsites_impression,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "topsites" AND name = "click")
    ) AS topsites_click,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "topsites" AND name = "pin")
    ) AS topsites_pin,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "topsites" AND name = "unpin")
    ) AS topsites_unpin,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "topsites" AND name = "add")
    ) AS topsites_add,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "topsites" AND name = "edit")
    ) AS topsites_edit,
    SUM(
      (
        SELECT
          COUNT(*)
        FROM
          UNNEST(events)
        WHERE
          category = "topsites"
          AND name = "show_privacy_click"
      )
    ) AS topsites_show_privacy_click,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "topsites" AND name = "dismiss")
    ) AS topsites_dismiss,
    SUM(
      (SELECT COUNT(*) FROM UNNEST(events) WHERE category = "topsites" AND name = "pref_changed")
    ) AS topsites_pref_changed
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
    topsites_enabled,
    topsites_sponsored_enabled,
    topsites_rows,
    topsites_sponsored_tiles_configured,
    pocket_click,
    pocket_impression,
    pocket_dismiss,
    pocket_save,
    topsites_impression,
    topsites_click,
    topsites_pin,
    topsites_unpin,
    topsites_add,
    topsites_edit,
    topsites_show_privacy_click,
    topsites_dismiss,
    topsites_pref_changed,
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
    COALESCE(CAST(COUNTIF(topsites_enabled IS TRUE) AS FLOAT64), 0) AS topsites_enabled_count_true,
    COALESCE(
      CAST(COUNTIF(topsites_sponsored_enabled IS TRUE) AS FLOAT64),
      0
    ) AS topsites_sponsored_enabled_count_true,
    COALESCE(CAST(AVG(topsites_rows) AS FLOAT64), 0) AS topsites_rows_avg,
    COALESCE(
      CAST(AVG(topsites_sponsored_tiles_configured) AS FLOAT64),
      0
    ) AS topsites_sponsored_tiles_configured_avg,
    COALESCE(CAST(AVG(pocket_click) AS FLOAT64), 0) AS pocket_click_avg,
    COALESCE(CAST(AVG(pocket_impression) AS FLOAT64), 0) AS pocket_impression_avg,
    COALESCE(CAST(AVG(pocket_dismiss) AS FLOAT64), 0) AS pocket_dismiss_avg,
    COALESCE(CAST(AVG(pocket_save) AS FLOAT64), 0) AS pocket_save_avg,
    COALESCE(CAST(AVG(topsites_impression) AS FLOAT64), 0) AS topsites_impression_avg,
    COALESCE(CAST(AVG(topsites_click) AS FLOAT64), 0) AS topsites_click_avg,
    COALESCE(CAST(AVG(topsites_pin) AS FLOAT64), 0) AS topsites_pin_avg,
    COALESCE(CAST(AVG(topsites_unpin) AS FLOAT64), 0) AS topsites_unpin_avg,
    COALESCE(CAST(AVG(topsites_add) AS FLOAT64), 0) AS topsites_add_avg,
    COALESCE(CAST(AVG(topsites_edit) AS FLOAT64), 0) AS topsites_edit_avg,
    COALESCE(
      CAST(AVG(topsites_show_privacy_click) AS FLOAT64),
      0
    ) AS topsites_show_privacy_click_avg,
    COALESCE(CAST(AVG(topsites_dismiss) AS FLOAT64), 0) AS topsites_dismiss_avg,
    COALESCE(CAST(AVG(topsites_pref_changed) AS FLOAT64), 0) AS topsites_pref_changed_avg,
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
      topsites_enabled_count_true,
      topsites_sponsored_enabled_count_true,
      topsites_rows_avg,
      topsites_sponsored_tiles_configured_avg,
      pocket_click_avg,
      pocket_impression_avg,
      pocket_dismiss_avg,
      pocket_save_avg,
      topsites_impression_avg,
      topsites_click_avg,
      topsites_pin_avg,
      topsites_unpin_avg,
      topsites_add_avg,
      topsites_edit_avg,
      topsites_show_privacy_click_avg,
      topsites_dismiss_avg,
      topsites_pref_changed_avg
    )
  )
