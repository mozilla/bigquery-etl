WITH clients_yearly AS (
  SELECT
    client_id,
    sample_id,
    submission_date,
    first_seen_date,
    days_since_seen,
    days_seen_bytes,
    days_since_first_seen,
  FROM
    fenix.clients_yearly
  WHERE
    {% if is_init() %}
      submission_date >= "2010-01-01"
    {% else %}
      submission_date = @submission_date
    {% endif %}
)
SELECT
  client_id,
  sample_id,
  clients_yearly.submission_date,
  clients_yearly.first_seen_date,
  clients_yearly.days_since_first_seen,
  clients_yearly.days_since_seen,
  clients_yearly.consecutive_days_seen,
  clients_yearly.days_seen_bytes,
  (
    SELECT
      LEAST(value, 10000)
    FROM
      UNNEST(ad_click_history)
    WHERE
      key = clients_yearly.submission_date
  ) AS ad_clicks_on_date,
  (
    SELECT
      SUM(LEAST(value, 10000))
    FROM
      UNNEST(ad_click_history)
    WHERE
      key <= clients_yearly.submission_date
  ) AS total_historic_ad_clicks,
  firefox_android_clients.first_reported_country,
  firefox_android_clients.first_reported_isp,
  firefox_android_clients.adjust_network,
  firefox_android_clients.install_source,
FROM
  clients_yearly
JOIN
  fenix.firefox_android_clients
USING
  (sample_id, client_id)
LEFT JOIN
  fenix.client_adclicks_history
USING
  (sample_id, client_id)
WHERE
    -- BrowserStack clients are bots, we don't want to accidentally report on them
  first_reported_isp != "BrowserStack"
    -- Remove clients who are new on this day, but have more/less than 1 day of activity
  AND NOT (days_since_first_seen = 0 AND BIT_COUNT(days_seen_bytes) != 1)
