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
    AND submission_date = @report_date --this is 9 days before {{ds}}
),
--Step 2: Get the first 7 days of these new clients' behavior from baseline_clients_last_seen
clients_last_seen_info AS (
  SELECT
    cls.client_id,
    cls.first_seen_date,
    cls.country,
    cls.submission_date,
    cls.days_since_seen,
    cls.active_hours_sum,
    cls.days_visited_1_uri_bits,
    cls.days_interacted_bits
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` cls
  JOIN
    clients_first_seen_9_days_ago clients
    ON cls.client_id = clients.client_id
  WHERE
    cls.submission_date
    BETWEEN @report_date
    AND DATE_ADD(@report_date, INTERVAL 6 DAY)
),
--Step 3: Get the first 7 days of these new clients' behavior from metrics_clients_last_seen
metrics_clients_last_seen AS (
  SELECT
    mcls.submission_date,
    mcls.client_id,
    mcls.search_with_ads_count_all
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics_clients_last_seen` mcls
  JOIN
    clients_first_seen_9_days_ago clients
    ON mcls.client_id = clients.client_id
  WHERE
    mcls.submission_date
    BETWEEN @report_date
    AND DATE_ADD(@report_date, INTERVAL 6 DAY)
),
--Step 4: Aggregate the info about these new clients first 7 days into 1 CTE
clients_last_seen_raw AS (
  SELECT
    COALESCE(a.client_id, b.client_id) AS client_id,
    COALESCE(a.submission_date, b.submission_date) AS submission_date,
    a.first_seen_date,
    a.country,
    a.days_since_seen,
    a.active_hours_sum,
    a.days_visited_1_uri_bits,
    a.days_interacted_bits,
    b.search_with_ads_count_all
  FROM
    clients_last_seen_info a
  FULL OUTER JOIN
    metrics_clients_last_seen b
    ON a.client_id = b.client_id
    AND a.submission_date = b.submission_date
),
--for each client, create 1 row with the summary of their behavior in first 7 days
client_activity_first_7_days AS (
  SELECT
    client_id,
    ANY_VALUE(
      first_seen_date
    ) AS first_seen_date, --date we got first main ping (potentially different than above first seen date)
    ANY_VALUE(country) AS country, --any country from their first day in clients_last_seen
    ANY_VALUE(
      CASE
        WHEN submission_date = DATE_ADD(first_seen_date, INTERVAL 6 DAY)
          THEN BIT_COUNT(days_visited_1_uri_bits & days_interacted_bits)
      END
    ) AS dou, --total # of days of activity during their first 7 days of main pings
  -- if a client doesn't send a ping on `submission_date` their last active day's value will be carried forward
  -- so we only take measurements from days that they send a ping.
    SUM(
      CASE
        WHEN days_since_seen = 0
          THEN COALESCE(active_hours_sum, 0)
        ELSE 0
      END
    ) AS active_hours_sum,
    SUM(
      CASE
        WHEN days_since_seen = 0
          THEN COALESCE(search_with_ads_count_all, 0)
        ELSE 0
      END
    ) AS search_with_ads_count_all
  FROM
    clients_last_seen_raw
  GROUP BY
    client_id
),
combined AS (
  SELECT
    cfs.client_id,
    cfs.first_seen_date,
    cfs.attribution_campaign,
    cfs.attribution_content,
    cfs.attribution_dltoken,
    cfs.attribution_medium,
    cfs.attribution_source,
    COALESCE(
      cls.country,
      cfs.country
    ) AS country, -- Conversion events & LTV are based on their first observed country in CLS, use that country if its available
    COALESCE(dou, 0) AS dou,
    COALESCE(active_hours_sum, 0) AS active_hours_sum,
    COALESCE(search_with_ads_count_all, 0) AS search_with_ads_count_all
  FROM
    clients_first_seen_9_days_ago AS cfs
  LEFT JOIN
    client_activity_first_7_days AS cls
    USING (client_id)
)
SELECT
  client_id,
  first_seen_date,
  attribution_campaign,
  attribution_content,
  attribution_dltoken,
  attribution_medium,
  attribution_source,
  @submission_date AS report_date,
  country,
  dou,
  active_hours_sum,
  search_with_ads_count_all,
  IF(search_with_ads_count_all > 0 AND dou >= 5, TRUE, FALSE) AS event_1,
  IF(search_with_ads_count_all > 0 AND dou >= 3, TRUE, FALSE) AS event_2,
  IF(active_hours_sum >= 0.4 AND dou >= 3, TRUE, FALSE) AS event_3,
  IF(dou >= 4, TRUE, FALSE) AS is_dau_at_least_4_of_first_7_days,
  IF(dou >= 3, TRUE, FALSE) AS is_dau_at_least_3_of_first_7_days,
  IF(dou >= 2, TRUE, FALSE) AS is_dau_at_least_2_of_first_7_days,
FROM
  combined
