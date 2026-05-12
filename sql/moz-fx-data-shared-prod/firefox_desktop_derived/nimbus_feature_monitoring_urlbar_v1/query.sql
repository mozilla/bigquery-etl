WITH rollouts_cte AS (
  SELECT
    normandy_slug AS rollout_slug
  FROM
    `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  WHERE
    "urlbar" IN UNNEST(feature_ids)
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
    client_info.client_id AS analysis_unit_id,
    LOGICAL_OR(metrics.boolean.urlbar_pref_suggest_sponsored) AS urlbar_pref_suggest_sponsored,
    LOGICAL_OR(
      metrics.boolean.urlbar_pref_suggest_nonsponsored
    ) AS urlbar_pref_suggest_nonsponsored,
    LOGICAL_OR(metrics.boolean.urlbar_pref_suggest_all) AS urlbar_pref_suggest_all,
    LOGICAL_OR(
      metrics.boolean.urlbar_pref_suggest_data_collection
    ) AS urlbar_pref_suggest_data_collection,
    LOGICAL_OR(
      metrics.boolean.urlbar_pref_suggest_online_available
    ) AS urlbar_pref_suggest_online_available,
    LOGICAL_OR(
      metrics.boolean.urlbar_pref_suggest_online_enabled
    ) AS urlbar_pref_suggest_online_enabled
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events`
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
    urlbar_pref_suggest_sponsored,
    urlbar_pref_suggest_nonsponsored,
    urlbar_pref_suggest_all,
    urlbar_pref_suggest_data_collection,
    urlbar_pref_suggest_online_available,
    urlbar_pref_suggest_online_enabled,
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
      CAST(COUNTIF(urlbar_pref_suggest_sponsored IS TRUE) AS FLOAT64),
      0
    ) AS urlbar_pref_suggest_sponsored_count_true,
    COALESCE(
      CAST(COUNTIF(urlbar_pref_suggest_nonsponsored IS TRUE) AS FLOAT64),
      0
    ) AS urlbar_pref_suggest_nonsponsored_count_true,
    COALESCE(
      CAST(COUNTIF(urlbar_pref_suggest_all IS TRUE) AS FLOAT64),
      0
    ) AS urlbar_pref_suggest_all_count_true,
    COALESCE(
      CAST(COUNTIF(urlbar_pref_suggest_data_collection IS TRUE) AS FLOAT64),
      0
    ) AS urlbar_pref_suggest_data_collection_count_true,
    COALESCE(
      CAST(COUNTIF(urlbar_pref_suggest_online_available IS TRUE) AS FLOAT64),
      0
    ) AS urlbar_pref_suggest_online_available_count_true,
    COALESCE(
      CAST(COUNTIF(urlbar_pref_suggest_online_enabled IS TRUE) AS FLOAT64),
      0
    ) AS urlbar_pref_suggest_online_enabled_count_true,
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
      urlbar_pref_suggest_sponsored_count_true,
      urlbar_pref_suggest_nonsponsored_count_true,
      urlbar_pref_suggest_all_count_true,
      urlbar_pref_suggest_data_collection_count_true,
      urlbar_pref_suggest_online_available_count_true,
      urlbar_pref_suggest_online_enabled_count_true
    )
  )
