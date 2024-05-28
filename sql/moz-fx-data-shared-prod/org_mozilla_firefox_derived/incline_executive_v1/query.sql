CREATE TEMP FUNCTION bucket_manufacturer(manufacturer STRING) AS (
  IF(LOWER(manufacturer) IN ('samsung', 'huawei'), LOWER(manufacturer), 'Other')
);

CREATE TEMP FUNCTION bucket_country(country STRING) AS (
  CASE
    WHEN country IN ('US', 'CA', 'DE', 'FR', 'GB')
      THEN [country, 'tier-1']
    WHEN country IN ('IN', 'CN', 'IR', 'BR', 'IE', 'ID')
      THEN [country, 'non-tier-1']
    ELSE ['non-tier-1']
  END
);

WITH fenix_clients_last_seen AS (
  SELECT
    * REPLACE ('firefox-preview nightly' AS normalized_channel),
  FROM
    org_mozilla_fenix.baseline_clients_last_seen
  UNION ALL
  SELECT
    * REPLACE ('preview nightly' AS normalized_channel),
  FROM
    org_mozilla_fenix_nightly.baseline_clients_last_seen
  UNION ALL
  SELECT
    * REPLACE ('release' AS normalized_channel),
  FROM
    org_mozilla_firefox.baseline_clients_last_seen
  UNION ALL
  SELECT
    * REPLACE ('beta' AS normalized_channel),
  FROM
    org_mozilla_firefox_beta.baseline_clients_last_seen
  UNION ALL
  SELECT
    * REPLACE ('nightly' AS normalized_channel),
  FROM
    org_mozilla_fennec_aurora.baseline_clients_last_seen
),
--
fennec_client_info AS (
  SELECT
    clients_last_seen.submission_date AS date,
    migrated_clients.fenix_client_id IS NOT NULL AS is_migrated,
    migrated_clients.submission_date = clients_last_seen.submission_date AS migrated_today,
    app_name,
    clients_last_seen.normalized_channel AS channel,
    SPLIT(device, '-')[OFFSET(0)] AS manufacturer,
    clients_last_seen.country,
    COALESCE(
      SAFE_CAST(REGEXP_EXTRACT(metadata_app_version, r"^[0-9]+") AS INT64) >= 68
      AND SAFE_CAST(osversion AS INT64) >= 21,
      FALSE
    ) AS can_migrate,
    udf.active_n_weeks_ago(days_seen_bits, 0) AS active_this_week,
    udf.active_n_weeks_ago(days_seen_bits, 1) AS active_last_week,
    udf.active_n_weeks_ago(days_created_profile_bits, 0) AS new_this_week,
    udf.active_n_weeks_ago(days_created_profile_bits, 1) AS new_last_week
  FROM
    telemetry_derived.core_clients_last_seen_v1 clients_last_seen
  LEFT JOIN
    org_mozilla_firefox.migrated_clients migrated_clients
    -- For Fennec, we only want to look at historical migration pings
    -- to see if this client has migrated. We use this to check if they
    -- were migrated today as well.
    ON clients_last_seen.client_id = migrated_clients.fennec_client_id
    AND clients_last_seen.submission_date >= migrated_clients.submission_date
  WHERE
    clients_last_seen.submission_date IN (
      @submission_date,
      -- We need to compare WoW retention delta YoY, and we don't want to backfill this table for an entire year.
      -- Workaround is to calculate YoY retention directly, by getting that WoW retention delta here.
      DATE_SUB(@submission_date, INTERVAL 1 YEAR),
      DATE_SUB(DATE_SUB(@submission_date, INTERVAL 1 YEAR), INTERVAL 1 WEEK)
    )
    AND app_name = 'Fennec'
    AND os = 'Android'
),
fenix_client_info AS (
  SELECT
    clients_last_seen.submission_date AS date,
    migrated_clients.fenix_client_id IS NOT NULL AS is_migrated,
    migrated_clients.submission_date = clients_last_seen.submission_date AS migrated_today,
    'Fenix' AS app_name,
    clients_last_seen.normalized_channel AS channel,
    device_manufacturer AS manufacturer,
    clients_last_seen.country,
    TRUE AS can_migrate,
    udf.active_n_weeks_ago(days_seen_bits, 0) AS active_this_week,
    udf.active_n_weeks_ago(days_seen_bits, 1) AS active_last_week,
    DATE_DIFF(clients_last_seen.submission_date, first_run_date, DAY)
    BETWEEN 0
    AND 6 AS new_this_week,
    DATE_DIFF(clients_last_seen.submission_date, first_run_date, DAY)
    BETWEEN 7
    AND 13 AS new_last_week
  FROM
    fenix_clients_last_seen clients_last_seen
  LEFT JOIN
    org_mozilla_firefox.migrated_clients migrated_clients
    -- For Fenix, we don't care if there's a delay in the migration ping, we know they
    -- have been migrated the entire time
    ON clients_last_seen.client_id = migrated_clients.fenix_client_id
  WHERE
    clients_last_seen.submission_date = @submission_date
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
    is_migrated_group AS is_migrated,
    channel_group AS channel,
    manufacturer_group AS manufacturer,
    country_group AS country,
    COUNT(*) AS active_count,
    COUNTIF(active_this_week AND migrated_today) AS new_migrations,
    COUNTIF(active_this_week AND can_migrate) AS can_migrate,
    COUNTIF(active_this_week AND NOT can_migrate) AS cannot_migrate,
    COUNTIF(active_last_week) AS active_previous,
    COUNTIF(active_this_week) AS active_current,
    COUNTIF(
      NOT new_last_week
      AND NOT new_this_week
      AND NOT active_last_week
      AND active_this_week
    ) AS resurrected,
    -- New users are only counted if they are active
    COUNTIF(new_this_week AND active_this_week) AS new_users,
    COUNTIF(
      NOT new_last_week
      AND NOT new_this_week
      AND active_last_week
      AND active_this_week
    ) AS established_returning,
    -- New returning users must have been active last week
    COUNTIF(new_last_week AND active_this_week AND active_last_week) AS new_returning,
    -- New churned users must have been active last week
    COUNTIF(new_last_week AND NOT active_this_week AND active_last_week) AS new_churned,
    COUNTIF(
      NOT new_last_week
      AND NOT new_this_week
      AND active_last_week
      AND NOT active_this_week
    ) AS established_churned
  FROM
    client_info
  -- These cross joins are a way to represent grouping
  -- sets for each one of these fields. They create
  -- a row with 'Overall' and that rows value
  CROSS JOIN
    UNNEST(['Overall', IF(is_migrated, 'Yes', 'No')]) AS is_migrated_group
  CROSS JOIN
    UNNEST(['Overall', channel]) AS channel_group
  CROSS JOIN
    UNNEST(['Overall', bucket_manufacturer(manufacturer)]) AS manufacturer_group
  CROSS JOIN
    UNNEST(ARRAY_CONCAT(['Overall'], bucket_country(country))) AS country_group
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
),
with_retention AS (
  SELECT
    * REPLACE (
    -- Churned users are a negative count, since they left the product
      -1 * established_churned AS established_churned,
      -1 * new_churned AS new_churned
    ),
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
),
_current AS (
  -- This is the current day's data. We will join this with the following CTEs
  -- that we will use for comparison.
  SELECT
    *
  FROM
    with_retention
  WHERE
    date = @submission_date
),
last_week AS (
  -- We want WoW for topline metrics, so get this data from last week
  SELECT
    * EXCEPT (date)
  FROM
    org_mozilla_firefox_derived.incline_executive_v1
  WHERE
    date = DATE_SUB(@submission_date, INTERVAL 1 WEEK)
),
last_year AS (
  -- We want YoY change in WoW retention, calculate last year's WoW retention here
  SELECT
    a.date,
    a.is_migrated,
    a.app_name,
    a.channel,
    a.manufacturer,
    a.country,
    a.established_returning_retention_rate - b.established_returning_retention_rate AS established_returning_retention_delta
  FROM
    with_retention a
  INNER JOIN
    with_retention b
    ON DATE_SUB(a.date, INTERVAL 1 WEEK) = b.date
    AND a.is_migrated = b.is_migrated
    AND a.app_name = b.app_name
    AND a.channel = b.channel
    AND a.manufacturer = b.manufacturer
    AND a.country = b.country
  WHERE
    a.date = DATE_SUB(@submission_date, INTERVAL 1 YEAR)
),
all_migrated_clients AS (
  -- This gives us the cumulative count of migrated clients
  SELECT
    'Fenix' AS app_name,
    channel_group AS channel,
    manufacturer_group AS manufacturer,
    country_group AS country,
    COUNT(*) AS cumulative_migration_count
  FROM
    org_mozilla_firefox.migrated_clients migrated_clients
  CROSS JOIN
    UNNEST(['Overall', normalized_channel]) AS channel_group
  CROSS JOIN
    UNNEST(['Overall', bucket_manufacturer(manufacturer)]) AS manufacturer_group
  CROSS JOIN
    UNNEST(ARRAY_CONCAT(['Overall'], bucket_country(country))) AS country_group
  WHERE
    submission_date <= @submission_date
  GROUP BY
    channel,
    manufacturer,
    country
)
SELECT
  _current.*,
  last_week.retention_rate AS retention_rate_previous,
  last_week.quick_ratio AS quick_ratio_previous,
  last_week.can_migrate AS can_migrate_previous,
  _current.established_returning_retention_rate - last_week.established_returning_retention_rate AS established_returning_retention_delta,
  last_year.established_returning_retention_delta AS established_returning_retention_delta_previous,
  all_migrated_clients.cumulative_migration_count
FROM
  _current
LEFT JOIN
  last_week
  USING (is_migrated, app_name, channel, manufacturer, country)
LEFT JOIN
  last_year
  USING (is_migrated, app_name, channel, manufacturer, country)
LEFT JOIN
  all_migrated_clients
  USING (app_name, channel, manufacturer, country)
