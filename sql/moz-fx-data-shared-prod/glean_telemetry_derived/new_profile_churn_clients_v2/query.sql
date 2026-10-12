WITH activity_range AS (
  SELECT
    last_seen.client_id,
    last_seen.sample_id,
    first_seen.first_seen_date,
    last_seen.submission_date,
    DATE_DIFF(last_seen.submission_date, first_seen.first_seen_date, DAY) AS days_since_first_seen,
    last_seen.days_desktop_active_bits
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` AS last_seen
  INNER JOIN
    `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` AS first_seen
    ON last_seen.client_id = first_seen.client_id
    AND last_seen.sample_id = first_seen.sample_id
  WHERE
    first_seen.first_seen_date = @submission_date
    AND last_seen.submission_date
    BETWEEN @submission_date
    AND DATE_ADD(@submission_date, INTERVAL 30 DAY)
),
  -- Latest date with data available; drives NULL logic for future day columns
max_available_date AS (
  SELECT
    MAX(submission_date) AS max_date
  FROM
    activity_range
),
  -- Client-level attributes from each client's baseline_clients_first_seen_v1 row
client_attributes AS (
  SELECT
    first_seen.client_id,
    IFNULL(first_seen.country, '??') AS country,
    COALESCE(REGEXP_EXTRACT(first_seen.locale, r'^(.+?)-'), first_seen.locale, NULL) AS locale,
    CASE
      WHEN LOWER(IFNULL(first_seen.isp, '')) = 'browserstack'
        THEN CONCAT('Firefox Desktop', ' ', first_seen.isp)
      WHEN LOWER(
          IFNULL(COALESCE(first_seen.distribution_id, distribution_mapping.distribution_id), '')
        ) = 'mozillaonline'
        THEN CONCAT(
            'Firefox Desktop',
            ' ',
            COALESCE(first_seen.distribution_id, distribution_mapping.distribution_id)
          )
      ELSE 'Firefox Desktop'
    END AS app_name,
    first_seen.app_display_version,
    `mozfun.norm.browser_version_info`(first_seen.app_display_version) AS browser_version_info,
    first_seen.normalized_channel,
    COALESCE(first_seen.distribution_id, distribution_mapping.distribution_id) AS distribution_id,
    CASE
      WHEN first_seen.distribution_id IS NOT NULL
        THEN "glean"
      WHEN distribution_mapping.distribution_id IS NOT NULL
        THEN "legacy"
      ELSE CAST(NULL AS STRING)
    END AS distribution_id_source,
    first_seen.normalized_os,
    first_seen.normalized_os_version,
    COALESCE(
      `mozfun.norm.glean_windows_version_info`(
        first_seen.normalized_os,
        first_seen.normalized_os_version,
        first_seen.windows_build_number
      ),
      first_seen.normalized_os_version
    ) AS os_version_build,
    CAST(
      `mozfun.norm.extract_version`(first_seen.normalized_os_version, "major") AS INTEGER
    ) AS os_version_major,
    CAST(
      `mozfun.norm.extract_version`(first_seen.normalized_os_version, "minor") AS INTEGER
    ) AS os_version_minor,
    EXTRACT(YEAR FROM first_seen.first_seen_date) AS first_seen_year,
    first_seen.attribution.campaign AS attribution_campaign,
    first_seen.attribution.content AS attribution_content,
    first_seen.attribution.medium AS attribution_medium,
    first_seen.attribution.source AS attribution_source,
    first_seen.attribution.term AS attribution_term,
    first_seen.distribution.name AS distribution_name,
    first_seen.attribution.campaign AS first_seen_attribution_campaign,
    first_seen.attribution.content AS first_seen_attribution_content,
    first_seen.attribution.medium AS first_seen_attribution_medium,
    first_seen.attribution.source AS first_seen_attribution_source,
    first_seen.attribution.term AS first_seen_attribution_term,
    first_seen.distribution.name AS first_seen_distribution_name,
    IF(
      LOWER(IFNULL(first_seen.isp, '')) <> 'browserstack'
      AND LOWER(
        IFNULL(COALESCE(first_seen.distribution_id, distribution_mapping.distribution_id), '')
      ) <> 'mozillaonline',
      TRUE,
      FALSE
    ) AS is_desktop,
    first_seen.policies_is_enterprise,
    first_seen.sample_id
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` AS first_seen
  LEFT JOIN
    `moz-fx-data-shared-prod.firefox_desktop_derived.desktop_dau_distribution_id_history_v1` AS distribution_mapping
    ON first_seen.client_id = distribution_mapping.client_id
    AND first_seen.first_seen_date = distribution_mapping.submission_date
    AND distribution_mapping.submission_date = @submission_date
  WHERE
    first_seen.first_seen_date = @submission_date
),
client_level AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    DATE_DIFF(max_date, first_seen_date, DAY) + 1 AS days_data_available,
    MAX(
      CASE
        WHEN days_since_first_seen = 0
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 0
          THEN FALSE
        ELSE NULL
      END
    ) AS day_0,
    MAX(
      CASE
        WHEN days_since_first_seen = 1
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 1
          THEN FALSE
        ELSE NULL
      END
    ) AS day_1,
    MAX(
      CASE
        WHEN days_since_first_seen = 2
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 2
          THEN FALSE
        ELSE NULL
      END
    ) AS day_2,
    MAX(
      CASE
        WHEN days_since_first_seen = 3
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 3
          THEN FALSE
        ELSE NULL
      END
    ) AS day_3,
    MAX(
      CASE
        WHEN days_since_first_seen = 4
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 4
          THEN FALSE
        ELSE NULL
      END
    ) AS day_4,
    MAX(
      CASE
        WHEN days_since_first_seen = 5
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 5
          THEN FALSE
        ELSE NULL
      END
    ) AS day_5,
    MAX(
      CASE
        WHEN days_since_first_seen = 6
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 6
          THEN FALSE
        ELSE NULL
      END
    ) AS day_6,
    MAX(
      CASE
        WHEN days_since_first_seen = 7
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 7
          THEN FALSE
        ELSE NULL
      END
    ) AS day_7,
    MAX(
      CASE
        WHEN days_since_first_seen = 8
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 8
          THEN FALSE
        ELSE NULL
      END
    ) AS day_8,
    MAX(
      CASE
        WHEN days_since_first_seen = 9
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 9
          THEN FALSE
        ELSE NULL
      END
    ) AS day_9,
    MAX(
      CASE
        WHEN days_since_first_seen = 10
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 10
          THEN FALSE
        ELSE NULL
      END
    ) AS day_10,
    MAX(
      CASE
        WHEN days_since_first_seen = 11
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 11
          THEN FALSE
        ELSE NULL
      END
    ) AS day_11,
    MAX(
      CASE
        WHEN days_since_first_seen = 12
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 12
          THEN FALSE
        ELSE NULL
      END
    ) AS day_12,
    MAX(
      CASE
        WHEN days_since_first_seen = 13
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 13
          THEN FALSE
        ELSE NULL
      END
    ) AS day_13,
    MAX(
      CASE
        WHEN days_since_first_seen = 14
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 14
          THEN FALSE
        ELSE NULL
      END
    ) AS day_14,
    MAX(
      CASE
        WHEN days_since_first_seen = 15
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 15
          THEN FALSE
        ELSE NULL
      END
    ) AS day_15,
    MAX(
      CASE
        WHEN days_since_first_seen = 16
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 16
          THEN FALSE
        ELSE NULL
      END
    ) AS day_16,
    MAX(
      CASE
        WHEN days_since_first_seen = 17
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 17
          THEN FALSE
        ELSE NULL
      END
    ) AS day_17,
    MAX(
      CASE
        WHEN days_since_first_seen = 18
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 18
          THEN FALSE
        ELSE NULL
      END
    ) AS day_18,
    MAX(
      CASE
        WHEN days_since_first_seen = 19
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 19
          THEN FALSE
        ELSE NULL
      END
    ) AS day_19,
    MAX(
      CASE
        WHEN days_since_first_seen = 20
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 20
          THEN FALSE
        ELSE NULL
      END
    ) AS day_20,
    MAX(
      CASE
        WHEN days_since_first_seen = 21
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 21
          THEN FALSE
        ELSE NULL
      END
    ) AS day_21,
    MAX(
      CASE
        WHEN days_since_first_seen = 22
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 22
          THEN FALSE
        ELSE NULL
      END
    ) AS day_22,
    MAX(
      CASE
        WHEN days_since_first_seen = 23
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 23
          THEN FALSE
        ELSE NULL
      END
    ) AS day_23,
    MAX(
      CASE
        WHEN days_since_first_seen = 24
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 24
          THEN FALSE
        ELSE NULL
      END
    ) AS day_24,
    MAX(
      CASE
        WHEN days_since_first_seen = 25
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 25
          THEN FALSE
        ELSE NULL
      END
    ) AS day_25,
    MAX(
      CASE
        WHEN days_since_first_seen = 26
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 26
          THEN FALSE
        ELSE NULL
      END
    ) AS day_26,
    MAX(
      CASE
        WHEN days_since_first_seen = 27
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 27
          THEN FALSE
        ELSE NULL
      END
    ) AS day_27,
    MAX(
      CASE
        WHEN days_since_first_seen = 28
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 28
          THEN FALSE
        ELSE NULL
      END
    ) AS day_28,
    MAX(
      CASE
        WHEN days_since_first_seen = 29
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 29
          THEN FALSE
        ELSE NULL
      END
    ) AS day_29,
    MAX(
      CASE
        WHEN days_since_first_seen = 30
          AND mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0
          THEN TRUE
        WHEN days_since_first_seen = 30
          THEN FALSE
        ELSE NULL
      END
    ) AS day_30
  FROM
    activity_range
  CROSS JOIN
    max_available_date
  GROUP BY
    client_id,
    sample_id,
    first_seen_date,
    max_date
)
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  first_seen_date AS cohort_date,
  days_data_available,
  -- Client attributes from the cohort date
  country,
  locale,
  app_name,
  app_display_version,
  browser_version_info.major_version AS app_version_major,
  browser_version_info.minor_version AS app_version_minor,
  browser_version_info.patch_revision AS app_version_patch_revision,
  browser_version_info.is_major_release AS app_version_is_major_release,
  normalized_channel,
  distribution_id,
  distribution_id_source,
  normalized_os,
  normalized_os_version,
  os_version_build,
  os_version_major,
  os_version_minor,
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
  -- No activity on days 1-4; at risk of churning by day 5
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
  -- No activity on days 2-6; at risk of churning by day 7
  -- NULL if day 6 data not yet available
  CASE
    WHEN days_data_available < 7
      THEN NULL
    WHEN day_2 = FALSE
      AND day_3 = FALSE
      AND day_4 = FALSE
      AND day_5 = FALSE
      AND day_6 = FALSE
      THEN TRUE
    ELSE FALSE
  END AS churn_risk_after_7_days,
  -- Active on day 0, then no activity for the next 28 days
  -- NULL if day 28 data not yet available
  CASE
    WHEN days_data_available < 29
      THEN NULL
    WHEN day_0 = TRUE
      AND day_1 = FALSE
      AND day_2 = FALSE
      AND day_3 = FALSE
      AND day_4 = FALSE
      AND day_5 = FALSE
      AND day_6 = FALSE
      AND day_7 = FALSE
      AND day_8 = FALSE
      AND day_9 = FALSE
      AND day_10 = FALSE
      AND day_11 = FALSE
      AND day_12 = FALSE
      AND day_13 = FALSE
      AND day_14 = FALSE
      AND day_15 = FALSE
      AND day_16 = FALSE
      AND day_17 = FALSE
      AND day_18 = FALSE
      AND day_19 = FALSE
      AND day_20 = FALSE
      AND day_21 = FALSE
      AND day_22 = FALSE
      AND day_23 = FALSE
      AND day_24 = FALSE
      AND day_25 = FALSE
      AND day_26 = FALSE
      AND day_27 = FALSE
      AND IFNULL(day_28, FALSE) = FALSE
      THEN TRUE
    ELSE FALSE
  END AS churned_after_1_day,
  -- Active on Day 1, then no activity for the next 28 days
  -- NULL if day 29 data not yet available
  CASE
    WHEN days_data_available < 30
      THEN NULL
    WHEN (day_1 = TRUE)
      AND day_2 = FALSE
      AND day_3 = FALSE
      AND day_4 = FALSE
      AND day_5 = FALSE
      AND day_6 = FALSE
      AND day_7 = FALSE
      AND day_8 = FALSE
      AND day_9 = FALSE
      AND day_10 = FALSE
      AND day_11 = FALSE
      AND day_12 = FALSE
      AND day_13 = FALSE
      AND day_14 = FALSE
      AND day_15 = FALSE
      AND day_16 = FALSE
      AND day_17 = FALSE
      AND day_18 = FALSE
      AND day_19 = FALSE
      AND day_20 = FALSE
      AND day_21 = FALSE
      AND day_22 = FALSE
      AND day_23 = FALSE
      AND day_24 = FALSE
      AND day_25 = FALSE
      AND day_26 = FALSE
      AND day_27 = FALSE
      AND IFNULL(day_28, FALSE) = FALSE
      AND IFNULL(day_29, FALSE) = FALSE
      THEN TRUE
    ELSE FALSE
  END AS churned_after_2_days,
  -- Client is in the cohort but never active on days 0-27
  -- NULL if day 27 data not yet available
  CASE
    WHEN days_data_available < 28
      THEN NULL
    WHEN day_0 = FALSE
      AND day_1 = FALSE
      AND day_2 = FALSE
      AND day_3 = FALSE
      AND day_4 = FALSE
      AND day_5 = FALSE
      AND day_6 = FALSE
      AND day_7 = FALSE
      AND day_8 = FALSE
      AND day_9 = FALSE
      AND day_10 = FALSE
      AND day_11 = FALSE
      AND day_12 = FALSE
      AND day_13 = FALSE
      AND day_14 = FALSE
      AND day_15 = FALSE
      AND day_16 = FALSE
      AND day_17 = FALSE
      AND day_18 = FALSE
      AND day_19 = FALSE
      AND day_20 = FALSE
      AND day_21 = FALSE
      AND day_22 = FALSE
      AND day_23 = FALSE
      AND day_24 = FALSE
      AND day_25 = FALSE
      AND day_26 = FALSE
      AND day_27 = FALSE
      THEN TRUE
    ELSE FALSE
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
  -- bits28 tracks 28 days (bits 0-27); clients inactive for 28+ days fall out of
  -- baseline_clients_last_seen entirely, producing NULL instead of FALSE. Coerce to
  -- FALSE when days_data_available confirms the date is in the past.
  IF(days_data_available >= 29 AND day_28 IS NULL, FALSE, day_28) AS day_28,
  IF(days_data_available >= 30 AND day_29 IS NULL, FALSE, day_29) AS day_29,
  IF(days_data_available >= 31 AND day_30 IS NULL, FALSE, day_30) AS day_30
FROM
  client_level
LEFT JOIN
  client_attributes
  USING (client_id, sample_id)
