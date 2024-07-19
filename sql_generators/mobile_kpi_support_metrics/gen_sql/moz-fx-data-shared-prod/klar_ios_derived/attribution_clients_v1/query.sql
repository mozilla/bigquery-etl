-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_ios.attribution_clients`
AS
WITH new_profiles AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
  FROM
    `moz-fx-data-shared-prod.klar_ios.active_users`
  WHERE
    submission_date = @submission_date
    AND is_new_profile
)
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
FROM
  new_profiles
