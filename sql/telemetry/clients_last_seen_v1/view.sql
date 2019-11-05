CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_last_seen_v1` AS
SELECT
  -- We cannot use UDFs in a view, so we paste the body of udf_bitpos(bits) literally here.
  CAST(SAFE.LOG(days_seen_bits & -days_seen_bits, 2) AS INT64) AS days_since_seen,
  CAST(SAFE.LOG(days_visited_5_uri_bits & -days_visited_5_uri_bits, 2) AS INT64) AS days_since_visited_5_uri,
  CAST(SAFE.LOG(days_opened_dev_tools_bits & -days_opened_dev_tools_bits, 2) AS INT64) AS days_since_opened_dev_tools,
  CAST(SAFE.LOG(days_created_profile_bits & -days_created_profile_bits, 2) AS INT64) AS days_since_created_profile,
  * REPLACE ( --
    ARRAY(SELECT * FROM UNNEST(active_addons.list)) AS active_addons,
    ARRAY(SELECT * FROM UNNEST(environment_settings_intl_accept_languages.list)) AS environment_settings_intl_accept_languages,
    ARRAY(SELECT * FROM UNNEST(environment_settings_intl_app_locales.list)) AS environment_settings_intl_app_locales,
    ARRAY(SELECT * FROM UNNEST(environment_settings_intl_available_locales.list)) AS environment_settings_intl_available_locales,
    ARRAY(SELECT * FROM UNNEST(environment_settings_intl_regional_prefs_locales.list)) AS environment_settings_intl_regional_prefs_locales,
    ARRAY(SELECT * FROM UNNEST(environment_settings_intl_requested_locales.list)) AS environment_settings_intl_requested_locales,
    ARRAY(SELECT * FROM UNNEST(environment_settings_intl_system_locales.list)) AS environment_settings_intl_system_locales,
    experiments.key_value AS experiments,
    SAFE.PARSE_DATE("%F", SUBSTR(profile_creation_date, 0, 10)) AS profile_creation_date,
    SAFE_CAST(sample_id AS INT64) AS sample_id,
    scalar_parent_devtools_accessibility_select_accessible_for_node_sum.key_value AS scalar_parent_devtools_accessibility_select_accessible_for_node_sum,
    ARRAY(
    SELECT
      AS STRUCT *,
      CAST(SAFE.LOG(bits & -bits, 2) AS INT64) AS days_since_seen
    FROM
      UNNEST(days_seen_in_experiment)) AS days_seen_in_experiment),
  -- TODO: Remove this temporary field once we transition the underlying
  -- clients_last_seen_v1 table to use integer sample_id.
  sample_id AS _sample_id_string
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.clients_last_seen_v1`
WHERE
  submission_date < DATE '2019-11-14'
UNION ALL
SELECT
  -- We cannot use UDFs in a view, so we paste the body of udf_bitpos(bits) literally here.
  CAST(SAFE.LOG(days_seen_bits & -days_seen_bits, 2) AS INT64) AS days_since_seen,
  CAST(SAFE.LOG(days_visited_5_uri_bits & -days_visited_5_uri_bits, 2) AS INT64) AS days_since_visited_5_uri,
  CAST(SAFE.LOG(days_opened_dev_tools_bits & -days_opened_dev_tools_bits, 2) AS INT64) AS days_since_opened_dev_tools,
  CAST(SAFE.LOG(days_created_profile_bits & -days_created_profile_bits, 2) AS INT64) AS days_since_created_profile,
  * REPLACE(
    ARRAY(
      SELECT AS STRUCT
        *,
        CAST(SAFE.LOG(bits & -bits, 2) AS INT64) AS days_since_seen
      FROM
        UNNEST(days_seen_in_experiment)
    ) AS days_seen_in_experiment
  ),
  -- TODO: Remove this temporary field once we transition the off the underlying
  -- clients_last_seen_v1 table that uses string sample_id.
  CAST(sample_id AS STRING) AS _sample_id_string
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v1`
WHERE
  submission_date >= DATE '2019-11-14'
