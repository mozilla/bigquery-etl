WITH rollouts_cte AS (
  SELECT
    normandy_slug AS rollout_slug
  FROM
    `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  WHERE
    "address-autofill-feature" IN UNNEST(feature_ids)
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
    LOGICAL_OR(metrics.boolean.formautofill_availability) AS formautofill_availability,
    SUM(
      (SELECT SUM(value) FROM UNNEST(metrics.labeled_counter.pwmgr_form_autofill_result))
    ) AS login_autofill,
    SUM(
      metrics.quantity.formautofill_addresses_autofill_profiles_count
    ) AS formautofill_addresses_autofill_profiles_count
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
    COUNTIF(
      event_category = "address"
      AND event_name = "detected_address_form"
    ) AS address_detected_address_form,
    COUNTIF(
      event_category = "address"
      AND event_name = "popup_shown_address_form"
    ) AS address_popup_shown_address_form,
    COUNTIF(
      event_category = "address"
      AND event_name = "filled_address_form"
    ) AS address_filled_address_form,
    COUNTIF(
      event_category = "address"
      AND event_name = "filled_on_fields_update_address_form"
    ) AS address_filled_on_fields_update_address_form,
    COUNTIF(
      event_category = "address"
      AND event_name = "filled_modified_address_form"
    ) AS address_filled_modified_address_form,
    COUNTIF(
      event_category = "address"
      AND event_name = "submitted_address_form"
    ) AS address_submitted_address_form,
    COUNTIF(
      event_category = "address"
      AND event_name = "cleared_address_form"
    ) AS address_cleared_address_form,
    COUNTIF(
      event_category = "address"
      AND event_name = "detected_address_form_ext"
    ) AS address_detected_address_form_ext,
    COUNTIF(
      event_category = "address"
      AND event_name = "filled_address_form_ext"
    ) AS address_filled_address_form_ext,
    COUNTIF(
      event_category = "address"
      AND event_name = "submitted_address_form_ext"
    ) AS address_submitted_address_form_ext,
    COUNTIF(
      event_category = "formautofill"
      AND event_name = "iframe_layout_detection"
    ) AS formautofill_iframe_layout_detection
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
    formautofill_availability,
    login_autofill,
    formautofill_addresses_autofill_profiles_count,
    address_detected_address_form,
    address_popup_shown_address_form,
    address_filled_address_form,
    address_filled_on_fields_update_address_form,
    address_filled_modified_address_form,
    address_submitted_address_form,
    address_cleared_address_form,
    address_detected_address_form_ext,
    address_filled_address_form_ext,
    address_submitted_address_form_ext,
    formautofill_iframe_layout_detection,
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
    COALESCE(
      CAST(COUNTIF(formautofill_availability IS TRUE) AS FLOAT64),
      0
    ) AS formautofill_availability_count_true,
    COALESCE(CAST(SUM(login_autofill) AS FLOAT64), 0) AS login_autofill_sum,
    COALESCE(CAST(AVG(login_autofill) AS FLOAT64), 0) AS login_autofill_avg,
    COALESCE(
      CAST(AVG(formautofill_addresses_autofill_profiles_count) AS FLOAT64),
      0
    ) AS formautofill_addresses_autofill_profiles_count_avg,
    COALESCE(
      CAST(AVG(address_detected_address_form) AS FLOAT64),
      0
    ) AS address_detected_address_form_avg,
    COALESCE(
      CAST(AVG(address_popup_shown_address_form) AS FLOAT64),
      0
    ) AS address_popup_shown_address_form_avg,
    COALESCE(
      CAST(AVG(address_filled_address_form) AS FLOAT64),
      0
    ) AS address_filled_address_form_avg,
    COALESCE(
      CAST(AVG(address_filled_on_fields_update_address_form) AS FLOAT64),
      0
    ) AS address_filled_on_fields_update_address_form_avg,
    COALESCE(
      CAST(AVG(address_filled_modified_address_form) AS FLOAT64),
      0
    ) AS address_filled_modified_address_form_avg,
    COALESCE(
      CAST(AVG(address_submitted_address_form) AS FLOAT64),
      0
    ) AS address_submitted_address_form_avg,
    COALESCE(
      CAST(AVG(address_cleared_address_form) AS FLOAT64),
      0
    ) AS address_cleared_address_form_avg,
    COALESCE(
      CAST(AVG(address_detected_address_form_ext) AS FLOAT64),
      0
    ) AS address_detected_address_form_ext_avg,
    COALESCE(
      CAST(AVG(address_filled_address_form_ext) AS FLOAT64),
      0
    ) AS address_filled_address_form_ext_avg,
    COALESCE(
      CAST(AVG(address_submitted_address_form_ext) AS FLOAT64),
      0
    ) AS address_submitted_address_form_ext_avg,
    COALESCE(
      CAST(AVG(formautofill_iframe_layout_detection) AS FLOAT64),
      0
    ) AS formautofill_iframe_layout_detection_avg,
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
  UNPIVOT(
    value FOR metric IN (
      formautofill_availability_count_true,
      login_autofill_sum,
      login_autofill_avg,
      formautofill_addresses_autofill_profiles_count_avg,
      address_detected_address_form_avg,
      address_popup_shown_address_form_avg,
      address_filled_address_form_avg,
      address_filled_on_fields_update_address_form_avg,
      address_filled_modified_address_form_avg,
      address_submitted_address_form_avg,
      address_cleared_address_form_avg,
      address_detected_address_form_ext_avg,
      address_filled_address_form_ext_avg,
      address_submitted_address_form_ext_avg,
      formautofill_iframe_layout_detection_avg
    )
  )
