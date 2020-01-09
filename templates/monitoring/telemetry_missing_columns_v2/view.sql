CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.telemetry_missing_columns_v2`
AS
WITH placeholder_table_names AS (
  SELECT
    DISTINCT table_name
  FROM
    `moz-fx-data-shared-prod`.telemetry_stable.INFORMATION_SCHEMA.TABLE_OPTIONS
  WHERE
    option_value LIKE '%placeholder_schema%'
),
extracted AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, DAY) AS day,
    'telemetry' AS document_namespace,
    REGEXP_EXTRACT(_TABLE_SUFFIX, r"^(.*)_v.*") AS document_type,
    REGEXP_EXTRACT(_TABLE_SUFFIX, r"^.*_v(.*)$") AS document_version,
    additional_properties
  FROM
    `moz-fx-data-shared-prod.telemetry_live.*`
  WHERE
    -- only observe full days of data
    (
      DATE(submission_timestamp) = DATE_SUB(current_date, INTERVAL 2 day)
      OR DATE(submission_timestamp) = DATE_SUB(current_date, INTERVAL 2 + 7 day)
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
        [
          "activeAddons",
          "userPrefs",
          "activeGMPlugins",
          "simpleMeasurements",
          "slowSQL",
          "slowSQLStartup",
          "XPI",
          "keyedHistograms",
          "histograms",
          "prio",
          "fileIOReports",
          "experiments",
          "keyedScalars",
          "scalars",
          "IceCandidatesStats"
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
    min(day) AS min_day,
    max(day) AS max_day
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
USING
  (document_namespace, document_type, document_version, path)
ORDER BY
  document_namespace,
  document_type,
  document_version,
  path
