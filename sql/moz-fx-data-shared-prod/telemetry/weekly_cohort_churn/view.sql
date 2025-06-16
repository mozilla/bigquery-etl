CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.weekly_cohort_churn`
AS
SELECT
  c.cohort_date_week,
  c.app_version,
  c.attribution_campaign,
  c.attribution_content,
  c.attribution_experiment,
  c.attribution_medium,
  c.attribution_source,
  c.attribution_variation,
  c.country,
  c.device_model,
  c.distribution_id,
  c.is_default_browser,
  c.locale,
  c.normalized_app_name,
  c.normalized_channel,
  c.normalized_os,
  c.normalized_os_version,
  c.adjust_ad_group,
  c.adjust_campaign,
  c.adjust_creative,
  c.adjust_network,
  c.play_store_attribution_campaign,
  c.play_store_attribution_medium,
  c.play_store_attribution_source,
  c.play_store_attribution_content,
  c.play_store_attribution_term,
  c.play_store_attribution_install_referrer_response,
  COUNT(DISTINCT c.client_id) AS total_users,
  COUNT(
    DISTINCT IF(
      a.submission_date
      BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 7 DAY)
      AND DATE_ADD(c.cohort_date_week, INTERVAL 13 DAY),
      c.client_id,
      NULL
    )
  ) AS returned_w1,
  COUNT(
    DISTINCT IF(
      a.submission_date
      BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 14 DAY)
      AND DATE_ADD(c.cohort_date_week, INTERVAL 20 DAY),
      c.client_id,
      NULL
    )
  ) AS returned_w2,
  COUNT(
    DISTINCT IF(
      a.submission_date
      BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 21 DAY)
      AND DATE_ADD(c.cohort_date_week, INTERVAL 27 DAY),
      c.client_id,
      NULL
    )
  ) AS returned_w3,
  COUNT(
    DISTINCT IF(
      a.submission_date
      BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 28 DAY)
      AND DATE_ADD(c.cohort_date_week, INTERVAL 34 DAY),
      c.client_id,
      NULL
    )
  ) AS returned_w4,
  -- Calculate churns and conditionally show churn rate only if sufficient time has passed
  COUNT(DISTINCT c.client_id) - COUNT(
    DISTINCT IF(
      a.submission_date
      BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 7 DAY)
      AND DATE_ADD(c.cohort_date_week, INTERVAL 13 DAY),
      c.client_id,
      NULL
    )
  ) AS churn_w1,
  IF(
    CURRENT_DATE >= DATE_ADD(c.cohort_date_week, INTERVAL 14 DAY),
    SAFE_DIVIDE(
      COUNT(DISTINCT c.client_id) - COUNT(
        DISTINCT IF(
          a.submission_date
          BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 7 DAY)
          AND DATE_ADD(c.cohort_date_week, INTERVAL 13 DAY),
          c.client_id,
          NULL
        )
      ),
      COUNT(DISTINCT c.client_id)
    ),
    NULL
  ) AS churn_rate_w1,
  COUNT(DISTINCT c.client_id) - COUNT(
    DISTINCT IF(
      a.submission_date
      BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 14 DAY)
      AND DATE_ADD(c.cohort_date_week, INTERVAL 20 DAY),
      c.client_id,
      NULL
    )
  ) AS churn_w2,
  IF(
    CURRENT_DATE >= DATE_ADD(c.cohort_date_week, INTERVAL 21 DAY),
    SAFE_DIVIDE(
      COUNT(DISTINCT c.client_id) - COUNT(
        DISTINCT IF(
          a.submission_date
          BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 14 DAY)
          AND DATE_ADD(c.cohort_date_week, INTERVAL 20 DAY),
          c.client_id,
          NULL
        )
      ),
      COUNT(DISTINCT c.client_id)
    ),
    NULL
  ) AS churn_rate_w2,
  COUNT(DISTINCT c.client_id) - COUNT(
    DISTINCT IF(
      a.submission_date
      BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 21 DAY)
      AND DATE_ADD(c.cohort_date_week, INTERVAL 27 DAY),
      c.client_id,
      NULL
    )
  ) AS churn_w3,
  IF(
    CURRENT_DATE >= DATE_ADD(c.cohort_date_week, INTERVAL 28 DAY),
    SAFE_DIVIDE(
      COUNT(DISTINCT c.client_id) - COUNT(
        DISTINCT IF(
          a.submission_date
          BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 21 DAY)
          AND DATE_ADD(c.cohort_date_week, INTERVAL 27 DAY),
          c.client_id,
          NULL
        )
      ),
      COUNT(DISTINCT c.client_id)
    ),
    NULL
  ) AS churn_rate_w3,
  COUNT(DISTINCT c.client_id) - COUNT(
    DISTINCT IF(
      a.submission_date
      BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 28 DAY)
      AND DATE_ADD(c.cohort_date_week, INTERVAL 34 DAY),
      c.client_id,
      NULL
    )
  ) AS churn_w4,
  IF(
    CURRENT_DATE >= DATE_ADD(c.cohort_date_week, INTERVAL 35 DAY),
    SAFE_DIVIDE(
      COUNT(DISTINCT c.client_id) - COUNT(
        DISTINCT IF(
          a.submission_date
          BETWEEN DATE_ADD(c.cohort_date_week, INTERVAL 28 DAY)
          AND DATE_ADD(c.cohort_date_week, INTERVAL 34 DAY),
          c.client_id,
          NULL
        )
      ),
      COUNT(DISTINCT c.client_id)
    ),
    NULL
  ) AS churn_rate_w4
FROM
  `moz-fx-data-shared-prod.telemetry_derived.cohort_weekly_cfs_staging_v1` c
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry_derived.cohort_weekly_active_clients_staging_v1` a
  ON a.client_id = c.client_id
GROUP BY
  c.cohort_date_week,
  c.app_version,
  c.attribution_campaign,
  c.attribution_content,
  c.attribution_experiment,
  c.attribution_medium,
  c.attribution_source,
  c.attribution_variation,
  c.country,
  c.device_model,
  c.distribution_id,
  c.is_default_browser,
  c.locale,
  c.normalized_app_name,
  c.normalized_channel,
  c.normalized_os,
  c.normalized_os_version,
  c.adjust_ad_group,
  c.adjust_campaign,
  c.adjust_creative,
  c.adjust_network,
  c.play_store_attribution_campaign,
  c.play_store_attribution_medium,
  c.play_store_attribution_source,
  c.play_store_attribution_content,
  c.play_store_attribution_term,
  c.play_store_attribution_install_referrer_response
