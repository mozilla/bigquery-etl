-- Query generated via `mobile_kpi_support_metrics` SQL generator.
WITH new_profiles AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
  FROM
    `moz-fx-data-shared-prod.focus_android.baseline_clients_first_seen`
  WHERE
    submission_date = @submission_date
    AND is_new_profile
)
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  normalized_channel,
FROM
  new_profiles
