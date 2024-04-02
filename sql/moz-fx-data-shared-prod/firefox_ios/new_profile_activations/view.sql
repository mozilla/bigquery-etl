CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.new_profile_activations`
AS
WITH new_profiles AS (
  SELECT
    client_id,
    first_seen_date,
    sample_id,
    channel,
  FROM
    firefox_ios.new_profiles
  WHERE
    -- submission_date = @submission_date
    -- AND DATE_DIFF(submission_date, first_seen_date, DAY) = 6
),
active_clients AS (
  SELECT
    client_id,
    first_seen_date,
    sample_id,
    "release" AS channel, -- TODO: add back once the upstream view has it
    COALESCE(ARRAY_LENGTH(
      mozfun.bits28.to_dates(mozfun.bits28.range(days_seen_bits, -5, 6), submission_date)
    ), 0) AS days_2_7,
  FROM
    backfills_staging_derived.active_users -- TODO: update once the view has been finalized and deployed to production
  WHERE
    DATE_DIFF(submission_date, first_seen_date, DAY) = 6
    AND app_name = "Firefox iOS"
),
clients_search AS (
  SELECT
    client_search.client_id,
    client_search.sample_id,
    client_search.channel,
    SUM(search_count) AS search_count
  FROM
    search_derived.mobile_search_clients_daily_v1 AS client_search
  INNER JOIN new_profiles ON
    client_search.client_id = new_profiles.client_id
    AND client_search.sample_id = new_profiles.sample_id
    AND client_search.channel = new_profiles.channel
    AND client_search.submission_date BETWEEN new_profiles.first_seen_date AND DATE_ADD(new_profiles.first_seen_date, INTERVAL 6 DAY)
  WHERE
    client_search.os = 'iOS'
    AND client_search.normalized_app_name = 'Fennec'
  GROUP BY
    client_id,
    sample_id,
    channel
)
SELECT
  submission_date AS activation_date,
  first_seen_date,
  client_id,
  sample_id,
  channel,
  (days_2_7 > 1 AND COALESCE(search_count, 0) > 0) AS is_activated,
FROM
  new_profiles
LEFT JOIN
  active_clients USING(client_id, sample_id, channel, first_seen_date)
LEFT JOIN
  clients_search USING (client_id, sample_id, channel)
