WITH activity_range AS (
  SELECT
    client_id,
    first_seen_date,
    submission_date,
    days_desktop_active_bits,
    sample_id,
    DATE_DIFF(submission_date, first_seen_date, DAY) AS days_since_first_seen
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
    sample_id
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` AS last_seen
  LEFT JOIN
    `moz-fx-data-shared-prod.firefox_desktop_derived.desktop_dau_distribution_id_history_v1` AS distribution_mapping
    USING (submission_date, client_id)
  LEFT JOIN
    `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` AS first_seen
    USING (client_id, sample_id)
  WHERE
      -- Pull attributes from each client's cohort date (first seen day) row
    last_seen.submission_date = last_seen.first_seen_date
    AND last_seen.submission_date >= "2026-01-01"
    AND last_seen.submission_date <= "2026-03-15"
),
client_level AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    DATE_DIFF(max_date, first_seen_date, DAY) AS days_data_available,
    days_since_last_data,
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
    max_date,
    days_since_last_data
)
SELECT
  client_id,
  sample_id,
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
  -- No activity on days 2-6; at risk of churning by day 7
  -- NULL if day 6 data not yet available
  CASE
    WHEN days_data_available < 6
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
    WHEN days_data_available < 28
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
      AND day_28 = FALSE
      THEN TRUE
      ELSE FALSE
  END AS churned_after_1_day,
  -- Active on days 0 and 1, then no activity for the next 28 days
  -- NULL if day 29 data not yet available
  CASE
    WHEN days_data_available < 29
      THEN NULL
    WHEN day_0 = TRUE
      AND day_1 = TRUE
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
      AND day_28 = FALSE
      AND day_29 = FALSE
      THEN "churned"
  END AS churned_after_2_days,
  -- Client is in the cohort but never active across the entire 30-day window
  -- NULL if day 30 data not yet available
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
      AND day_28 = FALSE
      AND day_29 = FALSE
      AND day_30 = FALSE
      THEN "immediately_churned"
    WHEN day_0 = TRUE
      OR day_1 = TRUE
      OR day_2 = TRUE
      OR day_3 = TRUE
      OR day_4 = TRUE
      OR day_5 = TRUE
      OR day_6 = TRUE
      OR day_7 = TRUE
      OR day_8 = TRUE
      OR day_9 = TRUE
      OR day_10 = TRUE
      OR day_11 = TRUE
      OR day_12 = TRUE
      OR day_13 = TRUE
      OR day_14 = TRUE
      OR day_15 = TRUE
      OR day_16 = TRUE
      OR day_17 = TRUE
      OR day_18 = TRUE
      OR day_19 = TRUE
      OR day_20 = TRUE
      OR day_21 = TRUE
      OR day_22 = TRUE
      OR day_23 = TRUE
      OR day_24 = TRUE
      OR day_25 = TRUE
      OR day_26 = TRUE
      OR day_27 = TRUE
      OR day_28 = TRUE
      OR day_29 = TRUE
      OR day_30 = TRUE
      THEN "not_immediately_churned"
    ELSE NULL
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
  USING (client_id, sample_id)
