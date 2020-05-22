CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.incline_executive_v1`
PARTITION BY
  (date)
AS
WITH columns AS (
  SELECT
    CAST(NULL AS DATE) AS date,
    CAST(NULL AS STRING) AS app_name,
    CAST(NULL AS STRING) AS is_migrated,
    CAST(NULL AS STRING) AS channel,
    CAST(NULL AS STRING) AS manufacturer,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS INT64) AS active_count,
    CAST(NULL AS INT64) AS is_migrated_count,
    CAST(NULL AS INT64) AS new_migrations,
    CAST(NULL AS INT64) AS can_migrate,
    CAST(NULL AS INT64) AS cannot_migrate,
    CAST(NULL AS INT64) AS active_previous,
    CAST(NULL AS INT64) AS active_current,
    CAST(NULL AS INT64) AS resurrected,
    CAST(NULL AS INT64) AS new_users,
    CAST(NULL AS INT64) AS established_returning,
    CAST(NULL AS INT64) AS new_returning,
    CAST(NULL AS INT64) AS new_churned,
    CAST(NULL AS INT64) AS established_churned,
    CAST(NULL AS FLOAT64) AS retention_rate,
    CAST(NULL AS FLOAT64) AS established_returning_retention_rate,
    CAST(NULL AS FLOAT64) AS new_returning_retention_rate,
    CAST(NULL AS FLOAT64) AS churn_rate,
    CAST(NULL AS FLOAT64) AS perc_of_active_resurrected,
    CAST(NULL AS FLOAT64) AS perc_of_active_new,
    CAST(NULL AS FLOAT64) AS perc_of_active_established_returning,
    CAST(NULL AS FLOAT64) AS perc_of_active_new_returning,
    CAST(NULL AS FLOAT64) AS quick_ratio,
    CAST(NULL AS FLOAT64) AS retention_rate_previous,
    CAST(NULL AS FLOAT64) AS quick_ratio_previous,
    CAST(NULL AS INT64) AS can_migrate_previous,
    CAST(NULL AS FLOAT64) AS established_returning_retention_delta,
    CAST(NULL AS FLOAT64) AS established_returning_retention_delta_previous,
    CAST(NULL AS INT64) AS cumulative_migration_count
)
SELECT
  *
FROM
  columns
WHERE
  FALSE
