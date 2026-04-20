WITH activity_range AS (
  SELECT
    client_id,
    first_seen_date,
    submission_date,
    days_desktop_active_bits
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen`
  WHERE
      -- Only include activity within each client's 30-day post-cohort window,
      -- up to the as_of_date
    submission_date
    BETWEEN first_seen_date
    AND DATE_ADD(first_seen_date, INTERVAL 30 DAY)
    AND submission_date >= "2026-01-01"
    AND submission_date <= "2026-03-15"
),
  -- Latest date with data available; drives NULL logic for future day columns
max_available_date AS (
  SELECT
    MAX(submission_date) AS max_date,
    DATE_DIFF(CURRENT_DATE(), MAX(submission_date), DAY) AS days_since_last_data
  FROM
    activity_range
),
  -- Client-level attributes from each client's cohort date row, with all joins
client_attributes AS (
  SELECT
    last_seen.client_id,
    IFNULL(last_seen.country, '??') AS country,
    IFNULL(last_seen.city, '??') AS city,
    COALESCE(REGEXP_EXTRACT(last_seen.locale, r'^(.+?)-'), last_seen.locale, NULL) AS locale,
    CASE
      WHEN LOWER(IFNULL(last_seen.isp, '')) = 'browserstack'
        THEN CONCAT('Firefox Desktop', ' ', last_seen.isp)
      WHEN LOWER(
          IFNULL(COALESCE(last_seen.distribution_id, distribution_mapping.distribution_id), '')
        ) = 'mozillaonline'
        THEN CONCAT(
            'Firefox Desktop',
            ' ',
            COALESCE(last_seen.distribution_id, distribution_mapping.distribution_id)
          )
      ELSE 'Firefox Desktop'
    END AS app_name,
    last_seen.app_display_version AS app_version,
    `mozfun.norm.browser_version_info`(
      last_seen.app_display_version
    ).major_version AS app_version_major,
    `mozfun.norm.browser_version_info`(
      last_seen.app_display_version
    ).minor_version AS app_version_minor,
    `mozfun.norm.browser_version_info`(
      last_seen.app_display_version
    ).patch_revision AS app_version_patch_revision,
    `mozfun.norm.browser_version_info`(
      last_seen.app_display_version
    ).is_major_release AS app_version_is_major_release,
    last_seen.normalized_channel AS channel,
    COALESCE(last_seen.distribution_id, distribution_mapping.distribution_id) AS distribution_id,
    CASE
      WHEN last_seen.distribution_id IS NOT NULL
        THEN "glean"
      WHEN distribution_mapping.distribution_id IS NOT NULL
        THEN "legacy"
      ELSE CAST(NULL AS STRING)
    END AS distribution_id_source,
    last_seen.normalized_os AS os,
    last_seen.normalized_os AS os_grouped,
    last_seen.normalized_os_version AS os_version,
    COALESCE(
      `mozfun.norm.glean_windows_version_info`(
        last_seen.normalized_os,
        last_seen.normalized_os_version,
        last_seen.windows_build_number
      ),
      last_seen.normalized_os_version
    ) AS os_version_build,
    CAST(
      `mozfun.norm.extract_version`(last_seen.normalized_os_version, "major") AS INTEGER
    ) AS os_version_major,
    CAST(
      `mozfun.norm.extract_version`(last_seen.normalized_os_version, "minor") AS INTEGER
    ) AS os_version_minor,
    CASE
      WHEN BIT_COUNT(last_seen.days_desktop_active_bits)
        BETWEEN 1
        AND 6
        THEN 'infrequent_user'
      WHEN BIT_COUNT(last_seen.days_desktop_active_bits)
        BETWEEN 7
        AND 13
        THEN 'casual_user'
      WHEN BIT_COUNT(last_seen.days_desktop_active_bits)
        BETWEEN 14
        AND 20
        THEN 'regular_user'
      WHEN BIT_COUNT(last_seen.days_desktop_active_bits) >= 21
        THEN 'core_user'
      ELSE 'other'
    END AS activity_segment,
    EXTRACT(YEAR FROM last_seen.first_seen_date) AS first_seen_year,
    last_seen.attribution.campaign AS attribution_campaign,
    last_seen.attribution.content AS attribution_content,
    last_seen.attribution.medium AS attribution_medium,
    last_seen.attribution.source AS attribution_source,
    last_seen.attribution.term AS attribution_term,
    last_seen.distribution.name AS distribution_name,
    first_seen.attribution.campaign AS first_seen_attribution_campaign,
    first_seen.attribution.content AS first_seen_attribution_content,
    first_seen.attribution.medium AS first_seen_attribution_medium,
    first_seen.attribution.source AS first_seen_attribution_source,
    first_seen.attribution.term AS first_seen_attribution_term,
    first_seen.distribution.name AS first_seen_distribution_name,
    IF(
      LOWER(IFNULL(last_seen.isp, '')) <> 'browserstack'
      AND LOWER(
        IFNULL(COALESCE(last_seen.distribution_id, distribution_mapping.distribution_id), '')
      ) <> 'mozillaonline',
      TRUE,
      FALSE
    ) AS is_desktop,
    last_seen.policies_is_enterprise,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` AS last_seen
  LEFT JOIN
    `moz-fx-data-shared-prod.firefox_desktop_derived.desktop_dau_distribution_id_history_v1` AS distribution_mapping
    USING (submission_date, client_id)
  LEFT JOIN
    `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` AS first_seen
    ON last_seen.client_id = first_seen.client_id
  WHERE
      -- Pull attributes from each client's cohort date (first seen day) row
    last_seen.submission_date = last_seen.first_seen_date
    AND last_seen.submission_date >= "2026-01-01"
    AND last_seen.submission_date <= "2026-03-15"
),
client_level AS (
  SELECT
    client_id,
    first_seen_date,
    DATE_DIFF(max_date, first_seen_date, DAY) AS days_data_available,
    days_since_last_data,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 0 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_0,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 1 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_1,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 2 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_2,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 3 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_3,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 4 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_4,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 5 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_5,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 6 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_6,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 7 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_7,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 8 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_8,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 9 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_9,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 10 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_10,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 11 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_11,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 12 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_12,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 13 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_13,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 14 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_14,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 15 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_15,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 16 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_16,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 17 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_17,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 18 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_18,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 19 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_19,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 20 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_20,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 21 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_21,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 22 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_22,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 23 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_23,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 24 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_24,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 25 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_25,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 26 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_26,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 27 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_27,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 28 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_28,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 29 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_29,
    MAX(
      IF(
        submission_date = DATE_ADD(first_seen_date, INTERVAL 30 DAY)
        AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0,
        "active",
        NULL
      )
    ) AS day_30
  FROM
    activity_range
  CROSS JOIN
    max_available_date
  GROUP BY
    client_id,
    first_seen_date,
    max_date,
    days_since_last_data
)
SELECT
  client_level.client_id,
  first_seen_date AS cohort_date,
  days_data_available,
  days_since_last_data,
  -- Client attributes from the cohort date
  country,
  city,
  locale,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
  channel,
  distribution_id,
  distribution_id_source,
  os,
  os_grouped,
  os_version,
  os_version_build,
  os_version_major,
  os_version_minor,
  activity_segment,
  first_seen_year,
  attribution_campaign,
  attribution_content,
  attribution_medium,
  attribution_source,
  attribution_term,
  distribution_name,
  first_seen_attribution_campaign,
  first_seen_attribution_content,
  first_seen_attribution_medium,
  first_seen_attribution_source,
  first_seen_attribution_term,
  first_seen_distribution_name,
  is_desktop,
  policies_is_enterprise,
  -- No activity on days 0-4; at risk of churning by day 5
  -- Clients active on day 0 or day 1 are excluded (they become churn_risk_after_7_days instead)
  -- NULL if day 4 data not yet available
  CASE
    WHEN days_data_available < 5
      THEN NULL
    WHEN day_1 IS FALSE
      AND day_2 IS FALSE
      AND day_3 IS FALSE
      AND day_4 IS FALSE
      THEN TRUE
     ELSE FALSE
  END AS churn_risk_after_5_days,
  -- Active on day 0 or day 1, then no activity on days 2-6; at risk of churning by day 7
  -- NULL if day 6 data not yet available
  CASE
    WHEN days_data_available < 6
      THEN NULL
    WHEN day_0 IS NULL
      AND day_1 IS NULL
      AND day_2 IS NULL
      AND day_3 IS NULL
      AND day_4 IS NULL
      AND day_5 IS NULL
      AND day_6 IS NULL
      THEN "churn_risk"
    WHEN (day_0 = "active" OR day_1 = "active")
      AND day_2 IS NULL
      AND day_3 IS NULL
      AND day_4 IS NULL
      AND day_5 IS NULL
      AND day_6 IS NULL
      THEN "churn_risk"
  END AS churn_risk_after_7_days,
  -- Active on day 0, then no activity for the next 28 days
  -- NULL if day 28 data not yet available
  CASE
    WHEN days_data_available < 28
      THEN NULL
    WHEN day_0 = "active"
      AND day_1 IS NULL
      AND day_2 IS NULL
      AND day_3 IS NULL
      AND day_4 IS NULL
      AND day_5 IS NULL
      AND day_6 IS NULL
      AND day_7 IS NULL
      AND day_8 IS NULL
      AND day_9 IS NULL
      AND day_10 IS NULL
      AND day_11 IS NULL
      AND day_12 IS NULL
      AND day_13 IS NULL
      AND day_14 IS NULL
      AND day_15 IS NULL
      AND day_16 IS NULL
      AND day_17 IS NULL
      AND day_18 IS NULL
      AND day_19 IS NULL
      AND day_20 IS NULL
      AND day_21 IS NULL
      AND day_22 IS NULL
      AND day_23 IS NULL
      AND day_24 IS NULL
      AND day_25 IS NULL
      AND day_26 IS NULL
      AND day_27 IS NULL
      AND day_28 IS NULL
      THEN "churned"
  END AS churned_after_1_day,
  -- Active on days 0 and 1, then no activity for the next 28 days
  -- NULL if day 29 data not yet available
  CASE
    WHEN days_data_available < 29
      THEN NULL
    WHEN day_0 = "active"
      AND day_1 = "active"
      AND day_2 IS NULL
      AND day_3 IS NULL
      AND day_4 IS NULL
      AND day_5 IS NULL
      AND day_6 IS NULL
      AND day_7 IS NULL
      AND day_8 IS NULL
      AND day_9 IS NULL
      AND day_10 IS NULL
      AND day_11 IS NULL
      AND day_12 IS NULL
      AND day_13 IS NULL
      AND day_14 IS NULL
      AND day_15 IS NULL
      AND day_16 IS NULL
      AND day_17 IS NULL
      AND day_18 IS NULL
      AND day_19 IS NULL
      AND day_20 IS NULL
      AND day_21 IS NULL
      AND day_22 IS NULL
      AND day_23 IS NULL
      AND day_24 IS NULL
      AND day_25 IS NULL
      AND day_26 IS NULL
      AND day_27 IS NULL
      AND day_28 IS NULL
      AND day_29 IS NULL
      THEN "churned"
  END AS churned_after_2_days,
  -- Client is in the cohort but never active across the entire 30-day window
  -- NULL if day 30 data not yet available
  CASE
    WHEN days_data_available < 30
      THEN NULL
    WHEN day_0 IS NULL
      AND day_1 IS NULL
      AND day_2 IS NULL
      AND day_3 IS NULL
      AND day_4 IS NULL
      AND day_5 IS NULL
      AND day_6 IS NULL
      AND day_7 IS NULL
      AND day_8 IS NULL
      AND day_9 IS NULL
      AND day_10 IS NULL
      AND day_11 IS NULL
      AND day_12 IS NULL
      AND day_13 IS NULL
      AND day_14 IS NULL
      AND day_15 IS NULL
      AND day_16 IS NULL
      AND day_17 IS NULL
      AND day_18 IS NULL
      AND day_19 IS NULL
      AND day_20 IS NULL
      AND day_21 IS NULL
      AND day_22 IS NULL
      AND day_23 IS NULL
      AND day_24 IS NULL
      AND day_25 IS NULL
      AND day_26 IS NULL
      AND day_27 IS NULL
      AND day_28 IS NULL
      AND day_29 IS NULL
      AND day_30 IS NULL
      THEN "immediately_churned"
  END AS immediately_churned,
  day_0,
  day_1,
  day_2,
  day_3,
  day_4,
  day_5,
  day_6,
  day_7,
  day_8,
  day_9,
  day_10,
  day_11,
  day_12,
  day_13,
  day_14,
  day_15,
  day_16,
  day_17,
  day_18,
  day_19,
  day_20,
  day_21,
  day_22,
  day_23,
  day_24,
  day_25,
  day_26,
  day_27,
  day_28,
  day_29,
  day_30
FROM
  client_level
LEFT JOIN
  client_attributes
  ON client_level.client_id = client_attributes.client_id;
