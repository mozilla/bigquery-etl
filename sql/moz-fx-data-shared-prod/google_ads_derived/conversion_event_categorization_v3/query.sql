--Step 1:  Get clients with a first seen date = submission date - 9 days
WITH clients_first_seen_9_days_ago AS (
  SELECT
    client_id,
    first_seen_date,
    country,
    attribution.campaign AS attribution_campaign,
    attribution.content AS attribution_content,
    attribution_dltoken,
    attribution.medium AS attribution_medium,
    attribution.source AS attribution_source
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.glean_baseline_clients_first_seen` cfs
  WHERE
    first_seen_date = @report_date --this is 9 days before {{ds}}
),
--Step 2: Get the first 7 days of these new clients' behavior after they were first seen
clients_last_seen_raw AS (
  SELECT
    cls.client_id,
    cls.first_seen_date,
    cls.country,
    cls.submission_date,
    cls.days_since_seen,
    cls.active_hours_sum, --not available yet
    cls.days_visited_1_uri_bits, --not available yet
    cls.days_interacted_bits,  --not available yet 
    cls.search_with_ads_count_all --not available yet
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` cls
  JOIN
    clients_first_seen_9_days_ago clients
    ON cls.client_id = clients.client_id
  WHERE
    cls.submission_date BETWEEN clients.first_seen_date AND DATE_ADD(clients.first_seen_date, INTERVAL 6 DAY)
), 