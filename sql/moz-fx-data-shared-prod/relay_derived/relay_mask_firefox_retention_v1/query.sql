WITH bridge AS (
  SELECT
    metrics.string.client_association_uid AS fxa_uid,
    client_info.client_id AS client_id,
    ROW_NUMBER() OVER (
      PARTITION BY
        metrics.string.client_association_uid
      ORDER BY
        submission_timestamp DESC
    ) AS rn
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.fx_accounts`
  WHERE
    DATE(submission_timestamp) >= DATE_SUB(@submission_date, INTERVAL 30 DAY)
    AND metrics.string.client_association_uid IS NOT NULL
    AND client_info.client_id IS NOT NULL
),
relay_masks AS (
  SELECT
    extras.string.fxa_id AS fxa_uid,
    MAX(
      COALESCE(extras.quantity.n_random_masks, 0) + COALESCE(extras.quantity.n_domain_masks, 0)
    ) AS total_masks,
    MAX(extras.string.premium_status) AS premium_status
  FROM
    `moz-fx-data-shared-prod.relay_backend.events_stream`
  WHERE
    DATE(submission_timestamp) >= DATE_SUB(@submission_date, INTERVAL 90 DAY)
    AND extras.string.fxa_id IS NOT NULL
  GROUP BY
    1
),
retention AS (
  SELECT
    client_id,
    metric_date,
    active_metric_date,
    retained_week_4,
    is_desktop,
    country,
    normalized_channel
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.desktop_retention_clients`
  WHERE
    is_desktop = TRUE
    AND submission_date = @submission_date
    AND metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
)
SELECT
  @submission_date AS submission_date,
  r.metric_date,
  r.country,
  r.normalized_channel,
  rm.premium_status AS relay_premium_status,
  rm.total_masks,
  CASE
    WHEN rm.fxa_uid IS NULL
      THEN 'No Relay account'
    WHEN rm.total_masks = 0
      THEN '0 masks'
    WHEN rm.total_masks
      BETWEEN 1
      AND 5
      THEN '1-5 masks'
    WHEN rm.total_masks
      BETWEEN 6
      AND 10
      THEN '6-10 masks'
    WHEN rm.total_masks
      BETWEEN 11
      AND 25
      THEN '11-25 masks'
    WHEN rm.total_masks
      BETWEEN 26
      AND 50
      THEN '26-50 masks'
    ELSE '51+ masks'
  END AS mask_count_tier,
  rm.fxa_uid IS NOT NULL AS has_relay_account,
  r.active_metric_date,
  r.retained_week_4,
  r.client_id
FROM
  retention r
LEFT JOIN
  bridge b
  ON r.client_id = b.client_id
  AND b.rn = 1
LEFT JOIN
  relay_masks rm
  ON b.fxa_uid = rm.fxa_uid
