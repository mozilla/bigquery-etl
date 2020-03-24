CREATE TEMP FUNCTION bucket_manufacturer(manufacturer STRING) AS (
  IF(
    manufacturer IN (
      'samsung',
      'huawei',
      'xiaomi',
      'lge',
      'motorola',
      'sony',
      'google',
      'oppo',
      'oneplus'
    ),
    manufacturer,
    'Other'
  )
);

CREATE TEMP FUNCTION bucket_country(country STRING) AS (
  IF(
    country IN ('US', 'CA', 'DE', 'IN', 'FR', 'CN', 'IR', 'BR', 'IE', 'GB', 'ID'),
    [country, 'Tier 1'],
    ['Other']
  )
);

WITH fennec_client_info AS (
  SELECT
    clients_last_seen.submission_date AS date,
    IF(migrated_clients.fenix_client_id IS NOT NULL, 'Yes', 'No') AS is_migrated,
    app_name,
    clients_last_seen.normalized_channel AS channel,
    SPLIT(device, '-')[OFFSET(0)] AS manufacturer,
    country,
    `moz-fx-data-shared-prod.udf.active_n_weeks_ago`(days_seen_bits, 0) AS active_this_week,
    `moz-fx-data-shared-prod.udf.active_n_weeks_ago`(days_seen_bits, 1) AS active_last_week,
    `moz-fx-data-shared-prod.udf.active_n_weeks_ago`(days_created_profile_bits, 0) AS new_this_week,
    `moz-fx-data-shared-prod.udf.active_n_weeks_ago`(days_created_profile_bits, 1) AS new_last_week
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.core_clients_last_seen_v1` clients_last_seen
  LEFT JOIN
    `moz-fx-data-shared-prod.org_mozilla_firefox.migrated_clients` migrated_clients
  ON
    clients_last_seen.client_id = migrated_clients.fennec_client_id
  WHERE
    clients_last_seen.submission_date = @submission_date
    AND migrated_clients.submission_date = @submission_date
    AND app_name = 'Fennec'
    AND os = 'Android'
),
fenix_client_info AS (
  SELECT
    clients_last_seen.submission_date AS date,
    IF(migrated_clients.fenix_client_id IS NOT NULL, 'Yes', 'No') AS is_migrated,
    'Fenix' AS app_name,
    clients_last_seen.normalized_channel AS channel,
    device_manufacturer AS manufacturer,
    country,
    `moz-fx-data-shared-prod.udf.active_n_weeks_ago`(days_seen_bits, 0) AS active_this_week,
    `moz-fx-data-shared-prod.udf.active_n_weeks_ago`(days_seen_bits, 1) AS active_last_week,
    DATE_DIFF(clients_last_seen.submission_date, first_run_date, DAY)
    BETWEEN 0
    AND 6 AS new_this_week,
    DATE_DIFF(clients_last_seen.submission_date, first_run_date, DAY)
    BETWEEN 7
    AND 13 AS new_last_week
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.clients_last_seen` clients_last_seen
  LEFT JOIN
    `moz-fx-data-shared-prod.org_mozilla_firefox.migrated_clients` migrated_clients
  ON
    clients_last_seen.client_id = migrated_clients.fenix_client_id
    AND clients_last_seen.submission_date <= migrated_clients.submission_date
  WHERE
    clients_last_seen.submission_date = @submission_date
    AND migrated_clients.submission_date <= @submission_date
),
client_info AS (
  SELECT
    *
  FROM
    fennec_client_info
  UNION ALL
  SELECT
    *
  FROM
    fenix_client_info
),
counts AS (
  SELECT
    date,
    app_name,
    COALESCE(is_migrated_group, is_migrated) AS is_migrated,
    COALESCE(channel_group, channel) AS channel,
    COALESCE(manufacturer_group, bucket_manufacturer(manufacturer)) AS manufacturer,
    COALESCE(country_group, bucketed_country) AS country,
    COUNT(*) AS active_count,
    COUNTIF(active_last_week) AS active_previous,
    COUNTIF(active_this_week) AS active_current,
    COUNTIF(
      NOT new_last_week
      AND NOT new_this_week
      AND NOT active_last_week
      AND active_this_week
    ) resurrected,
    COUNTIF(new_this_week) AS new_users,
    COUNTIF(
      NOT new_last_week
      AND NOT new_this_week
      AND active_last_week
      AND active_this_week
    ) AS established_returning,
    COUNTIF(new_last_week AND active_this_week) AS new_returning,
    COUNTIF(new_last_week AND NOT active_this_week) AS new_churned,
    COUNTIF(
      NOT new_last_week
      AND NOT new_this_week
      AND active_last_week
      AND NOT active_this_week
    ) AS established_churned -- 6
  FROM
    client_info
  -- These cross joins are a way to represent grouping
  -- sets for each one of these fields. They create
  -- a row with 'Overall' for value and NULL for value
  -- for each row, which allows us to count that row
  -- for both fields. When we COALESCE the null above,
  -- we end up grouping on both that rows natural value,
  -- and 'Overall', which contains all rows.
  CROSS JOIN
    UNNEST(['Overall', NULL]) AS is_migrated_group
  CROSS JOIN
    UNNEST(['Overall', NULL]) AS channel_group
  CROSS JOIN
    UNNEST(['Overall', NULL]) AS manufacturer_group
  CROSS JOIN
    UNNEST(['Overall', NULL]) AS country_group
  CROSS JOIN
    -- Rows with Tier 1 countries need to be part of
    -- both their country and part of Tier 1
    UNNEST(bucket_country(country)) AS bucketed_country
  WHERE
    active_last_week
    OR active_this_week
  GROUP BY
    date,
    is_migrated,
    app_name,
    channel,
    manufacturer,
    country
)
SELECT
  * EXCEPT (established_churned, new_churned),
  -1 * established_churned AS established_churned,
  -1 * new_churned AS new_churned,
  SAFE_DIVIDE((established_returning + new_returning), active_previous) AS retention_rate,
  SAFE_DIVIDE(
    established_returning,
    (established_returning + established_churned)
  ) AS established_returning_retention_rate,
  SAFE_DIVIDE(new_returning, (new_returning + new_churned)) AS new_returning_retention_rate,
  SAFE_DIVIDE((established_churned + new_churned), active_previous) AS churn_rate,
  SAFE_DIVIDE(resurrected, active_current) AS perc_of_active_resurrected,
  SAFE_DIVIDE(new_users, active_current) AS perc_of_active_new,
  SAFE_DIVIDE(established_returning, active_current) AS perc_of_active_established_returning,
  SAFE_DIVIDE(new_returning, active_current) AS perc_of_active_new_returning,
  SAFE_DIVIDE((new_users + resurrected), (established_churned + new_churned)) AS quick_ratio,
FROM
  counts
