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
    @submission_date AS submission_date,
    'telemetry' AS document_namespace,
    `moz-fx-data-shared-prod`.udf.extract_document_type(_TABLE_SUFFIX) AS document_type,
    `moz-fx-data-shared-prod`.udf.extract_document_version(_TABLE_SUFFIX) AS document_version,
    additional_properties
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.*`
  WHERE
    -- only observe full days of data
    DATE(submission_timestamp) = @submission_date
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
    submission_date,
    document_namespace,
    document_type,
    document_version,
    path
)
SELECT
  submission_date,
  document_namespace,
  document_type,
  document_version,
  path,
  path_count
FROM
  transformed
