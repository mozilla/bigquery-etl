CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.telemetry_missing_columns_v2`
AS
WITH placeholder_table_names AS (
  SELECT DISTINCT
    table_name
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE
    option_value LIKE r'%placeholder\_schema%'
),
extracted AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, DAY) AS day,
    'telemetry' AS document_namespace,
    `moz-fx-data-shared-prod`.udf.extract_document_type(_TABLE_SUFFIX) AS document_type,
    `moz-fx-data-shared-prod`.udf.extract_document_version(_TABLE_SUFFIX) AS document_version,
    additional_properties
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.*`
  WHERE
    -- only observe full days of data
    (
      DATE(submission_timestamp) = DATE_SUB(current_date, INTERVAL 1 day)
      OR DATE(submission_timestamp) = DATE_SUB(current_date, INTERVAL(1 + 7) day)
    )
    -- https://cloud.google.com/bigquery/docs/querying-wildcard-tables#filtering_selected_tables_using_table_suffix
    -- exclude pings derived from main schema to save on space, 300GB vs 3TB
    AND _TABLE_SUFFIX NOT IN (
      'main_v4',
      'saved_session_v4',
      'first_shutdown_v4',
      'main_v5',
      'saved_session_v5',
      'first_shutdown_v5'
    )
    AND _TABLE_SUFFIX NOT IN (SELECT * FROM placeholder_table_names)
),
transformed AS (
  SELECT
    * EXCEPT (additional_properties),
    COUNT(*) AS path_count
  FROM
    extracted,
    UNNEST(
      `moz-fx-data-shared-prod`.udf_js.json_extract_missing_cols(
        additional_properties,
        [],
        -- Manually curated list of known missing sections. The process to
        -- generate list is to change the project for the live table to
        -- moz-fx-data-shar-nonprod-efed to obtain a 1% sample. Run this query
        -- except with the following list empty. This generates a total of
        -- O(1e5) total distinct (document, path) rows. All nodes with a large
        -- number of subpaths are added to the following list, e.g. activeAddons
        -- or histograms. The list is then curated such that there are
        -- approximately less than 1000 distinct paths.
        [
          -- common environment
          "activeAddons",
          "userPrefs",
          "experiments",
          -- common measures
          "keyedScalars",
          "scalars",
          "histograms",
          "keyedHistograms"
        ]
      )
    ) AS path
  GROUP BY
    day,
    document_namespace,
    document_type,
    document_version,
    path
),
min_max AS (
  SELECT
    MIN(day) AS min_day,
    MAX(day) AS max_day
  FROM
    transformed
),
reference AS (
  SELECT
    * EXCEPT (day, path_count),
    path_count AS path_count_ref
  FROM
    transformed
  WHERE
    day IN (SELECT min_day FROM min_max)
),
observed AS (
  SELECT
    * EXCEPT (path_count),
    path_count AS path_count_obs
  FROM
    transformed
  WHERE
    day IN (SELECT max_day FROM min_max)
)
SELECT
  day,
  * EXCEPT (day),
  path_count_obs - path_count_ref AS diff
FROM
  reference
FULL JOIN
  observed
  USING (document_namespace, document_type, document_version, path)
ORDER BY
  document_namespace,
  document_type,
  document_version,
  path
