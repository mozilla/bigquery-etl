WITH rollouts_cte AS (
  SELECT
    normandy_slug AS rollout_slug
  FROM
    `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  WHERE
    "newTabSectionsExperiment" IN UNNEST(feature_ids)
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
    profile_group_id AS analysis_unit_id,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    CAST(submission_timestamp AS DATE) = @submission_date
  GROUP BY
    analysis_unit_id
),
source_table_cte_3 AS (
  SELECT
    metrics.uuid.legacy_telemetry_profile_group_id AS analysis_unit_id,
    SUM(
      (
        SELECT
          COUNT(*)
        FROM
          UNNEST(events)
        WHERE
          category = "newtab"
          AND name = "sections_impression"
      )
    ) AS newtab_sections_impression,
    SUM(
      (
        SELECT
          COUNT(*)
        FROM
          UNNEST(events)
        WHERE
          category = "newtab"
          AND name = "sections_follow_section"
      )
    ) AS newtab_sections_follow_section,
    SUM(
      (
        SELECT
          COUNT(*)
        FROM
          UNNEST(events)
        WHERE
          category = "newtab"
          AND name = "sections_unfollow_section"
      )
    ) AS newtab_sections_unfollow_section,
    SUM(
      (
        SELECT
          COUNT(*)
        FROM
          UNNEST(events)
        WHERE
          category = "newtab"
          AND name = "sections_block_section"
      )
    ) AS newtab_sections_block_section,
    SUM(
      (
        SELECT
          COUNT(*)
        FROM
          UNNEST(events)
        WHERE
          category = "newtab"
          AND name = "sections_unblock_section"
      )
    ) AS newtab_sections_unblock_section
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
    newtab_sections_impression,
    newtab_sections_follow_section,
    newtab_sections_unfollow_section,
    newtab_sections_block_section,
    newtab_sections_unblock_section,
  FROM
    source_table_cte_1,
    UNNEST(rollout_slugs) AS rollout_slug
  JOIN
    source_table_cte_2
    USING (analysis_unit_id)
  JOIN
    source_table_cte_3
    USING (analysis_unit_id)
),
aggregates_cte AS (
  SELECT
    rollout_slug,
    dimensions,
    COUNT(*) AS count_analysis_unit_ids,
    COALESCE(CAST(AVG(newtab_sections_impression) AS FLOAT64), 0) AS newtab_sections_impression_avg,
    COALESCE(
      CAST(AVG(newtab_sections_follow_section) AS FLOAT64),
      0
    ) AS newtab_sections_follow_section_avg,
    COALESCE(
      CAST(AVG(newtab_sections_unfollow_section) AS FLOAT64),
      0
    ) AS newtab_sections_unfollow_section_avg,
    COALESCE(
      CAST(AVG(newtab_sections_block_section) AS FLOAT64),
      0
    ) AS newtab_sections_block_section_avg,
    COALESCE(
      CAST(AVG(newtab_sections_unblock_section) AS FLOAT64),
      0
    ) AS newtab_sections_unblock_section_avg,
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
      newtab_sections_impression_avg,
      newtab_sections_follow_section_avg,
      newtab_sections_unfollow_section_avg,
      newtab_sections_block_section_avg,
      newtab_sections_unblock_section_avg
    )
  )
