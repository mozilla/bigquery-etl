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
    -- The day 2 row covers days 0-2 and the day 30 row covers days 3-30
    AND last_seen.submission_date IN (
      DATE_ADD(@submission_date, INTERVAL 2 DAY),
      DATE_ADD(@submission_date, INTERVAL 30 DAY)
    )
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
    DATE_DIFF(max_date, first_seen_date, DAY) AS cohort_age,
    MAX(IF(days_since_first_seen = 2, days_desktop_active_bits, NULL)) AS day2_bits,
    MAX(IF(submission_date = max_date, days_desktop_active_bits, NULL)) AS later_bits
  FROM
    activity_range
  CROSS JOIN
    max_available_date
  GROUP BY
    client_id,
    sample_id,
    first_seen_date,
    max_date
),
  -- One activity pattern per client, with day k at bit cohort_age - k; the day 2 row
  -- adds days 0-2 once the latest row no longer covers them
client_bits AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    days_data_available,
    cohort_age,
    COALESCE(later_bits, 0) | IF(
      cohort_age >= 28,
      (COALESCE(day2_bits, 0) & 7) << (cohort_age - 2),
      0
    ) AS cohort_bits
  FROM
    client_level
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
  IF(
    cohort_age >= 4,
    NOT mozfun.bits28.active_in_range(cohort_bits, 1 - cohort_age, 4),
    NULL
  ) AS churn_risk_after_5_days,
  -- No activity on days 2-6; at risk of churning by day 7
  -- NULL if day 6 data not yet available
  IF(
    cohort_age >= 6,
    NOT mozfun.bits28.active_in_range(cohort_bits, 2 - cohort_age, 5),
    NULL
  ) AS churn_risk_after_7_days,
  -- Active on day 0, then no activity for the next 28 days
  -- NULL if day 28 data not yet available
  IF(
    cohort_age >= 28,
    mozfun.bits28.active_in_range(cohort_bits, -cohort_age, 1)
    AND NOT mozfun.bits28.active_in_range(cohort_bits, 1 - cohort_age, 28),
    NULL
  ) AS churned_after_1_day,
  -- Active on Day 1, then no activity for the next 28 days
  -- NULL if day 29 data not yet available
  IF(
    cohort_age >= 29,
    mozfun.bits28.active_in_range(cohort_bits, 1 - cohort_age, 1)
    AND NOT mozfun.bits28.active_in_range(cohort_bits, 2 - cohort_age, 28),
    NULL
  ) AS churned_after_2_days,
  -- Client is in the cohort but never active on days 0-27
  -- NULL if day 27 data not yet available
  IF(
    cohort_age >= 27,
    NOT mozfun.bits28.active_in_range(cohort_bits, -cohort_age, 28),
    NULL
  ) AS immediately_churned,
  -- Active on day k since first seen; NULL if day k data not yet available
  IF(cohort_age >= 0, mozfun.bits28.active_in_range(cohort_bits, 0 - cohort_age, 1), NULL) AS day_0,
  IF(cohort_age >= 1, mozfun.bits28.active_in_range(cohort_bits, 1 - cohort_age, 1), NULL) AS day_1,
  IF(cohort_age >= 2, mozfun.bits28.active_in_range(cohort_bits, 2 - cohort_age, 1), NULL) AS day_2,
  IF(cohort_age >= 3, mozfun.bits28.active_in_range(cohort_bits, 3 - cohort_age, 1), NULL) AS day_3,
  IF(cohort_age >= 4, mozfun.bits28.active_in_range(cohort_bits, 4 - cohort_age, 1), NULL) AS day_4,
  IF(cohort_age >= 5, mozfun.bits28.active_in_range(cohort_bits, 5 - cohort_age, 1), NULL) AS day_5,
  IF(cohort_age >= 6, mozfun.bits28.active_in_range(cohort_bits, 6 - cohort_age, 1), NULL) AS day_6,
  IF(cohort_age >= 7, mozfun.bits28.active_in_range(cohort_bits, 7 - cohort_age, 1), NULL) AS day_7,
  IF(cohort_age >= 8, mozfun.bits28.active_in_range(cohort_bits, 8 - cohort_age, 1), NULL) AS day_8,
  IF(cohort_age >= 9, mozfun.bits28.active_in_range(cohort_bits, 9 - cohort_age, 1), NULL) AS day_9,
  IF(
    cohort_age >= 10,
    mozfun.bits28.active_in_range(cohort_bits, 10 - cohort_age, 1),
    NULL
  ) AS day_10,
  IF(
    cohort_age >= 11,
    mozfun.bits28.active_in_range(cohort_bits, 11 - cohort_age, 1),
    NULL
  ) AS day_11,
  IF(
    cohort_age >= 12,
    mozfun.bits28.active_in_range(cohort_bits, 12 - cohort_age, 1),
    NULL
  ) AS day_12,
  IF(
    cohort_age >= 13,
    mozfun.bits28.active_in_range(cohort_bits, 13 - cohort_age, 1),
    NULL
  ) AS day_13,
  IF(
    cohort_age >= 14,
    mozfun.bits28.active_in_range(cohort_bits, 14 - cohort_age, 1),
    NULL
  ) AS day_14,
  IF(
    cohort_age >= 15,
    mozfun.bits28.active_in_range(cohort_bits, 15 - cohort_age, 1),
    NULL
  ) AS day_15,
  IF(
    cohort_age >= 16,
    mozfun.bits28.active_in_range(cohort_bits, 16 - cohort_age, 1),
    NULL
  ) AS day_16,
  IF(
    cohort_age >= 17,
    mozfun.bits28.active_in_range(cohort_bits, 17 - cohort_age, 1),
    NULL
  ) AS day_17,
  IF(
    cohort_age >= 18,
    mozfun.bits28.active_in_range(cohort_bits, 18 - cohort_age, 1),
    NULL
  ) AS day_18,
  IF(
    cohort_age >= 19,
    mozfun.bits28.active_in_range(cohort_bits, 19 - cohort_age, 1),
    NULL
  ) AS day_19,
  IF(
    cohort_age >= 20,
    mozfun.bits28.active_in_range(cohort_bits, 20 - cohort_age, 1),
    NULL
  ) AS day_20,
  IF(
    cohort_age >= 21,
    mozfun.bits28.active_in_range(cohort_bits, 21 - cohort_age, 1),
    NULL
  ) AS day_21,
  IF(
    cohort_age >= 22,
    mozfun.bits28.active_in_range(cohort_bits, 22 - cohort_age, 1),
    NULL
  ) AS day_22,
  IF(
    cohort_age >= 23,
    mozfun.bits28.active_in_range(cohort_bits, 23 - cohort_age, 1),
    NULL
  ) AS day_23,
  IF(
    cohort_age >= 24,
    mozfun.bits28.active_in_range(cohort_bits, 24 - cohort_age, 1),
    NULL
  ) AS day_24,
  IF(
    cohort_age >= 25,
    mozfun.bits28.active_in_range(cohort_bits, 25 - cohort_age, 1),
    NULL
  ) AS day_25,
  IF(
    cohort_age >= 26,
    mozfun.bits28.active_in_range(cohort_bits, 26 - cohort_age, 1),
    NULL
  ) AS day_26,
  IF(
    cohort_age >= 27,
    mozfun.bits28.active_in_range(cohort_bits, 27 - cohort_age, 1),
    NULL
  ) AS day_27,
  IF(
    cohort_age >= 28,
    mozfun.bits28.active_in_range(cohort_bits, 28 - cohort_age, 1),
    NULL
  ) AS day_28,
  IF(
    cohort_age >= 29,
    mozfun.bits28.active_in_range(cohort_bits, 29 - cohort_age, 1),
    NULL
  ) AS day_29,
  IF(
    cohort_age >= 30,
    mozfun.bits28.active_in_range(cohort_bits, 30 - cohort_age, 1),
    NULL
  ) AS day_30
FROM
  client_bits
LEFT JOIN
  client_attributes
  USING (client_id, sample_id)
