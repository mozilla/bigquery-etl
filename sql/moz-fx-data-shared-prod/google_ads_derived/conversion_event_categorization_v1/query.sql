DECLARE first_cohort_date DATE DEFAULT DATE(2023, 11, 1);

DECLARE last_cohort_date DATE DEFAULT DATE_SUB(
  CURRENT_DATE,
  INTERVAL 8 DAY
); -- want to make sure the last cohort we are reporting has had their full first 7 days
WITH clients_first_seen AS (
  SELECT
    client_id,
    first_seen_date,
    country,
    attribution_campaign,
    attribution_content,
    attribution_dltoken,
    attribution_medium,
    attribution_source
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_first_seen` --contains all new clients, including those that never sent a main ping
  WHERE
    first_seen_date
    BETWEEN first_cohort_date
    AND last_cohort_date
),
clients_last_seen AS (
  SELECT
    client_id,
  -- note that this is the date we got each client's first main ping, which is potentially a different date from the first_seen_date above.
    ANY_VALUE(first_seen_date) AS first_seen_date,
  -- just the country value from their first main ping day
    ANY_VALUE(CASE WHEN first_seen_date = submission_date THEN country END) AS country,
  -- the date we would report on their conversion events (after we get data from their 7th day)
    ANY_VALUE(
      IF(submission_date = DATE_ADD(first_seen_date, INTERVAL 6 DAY), submission_date, NULL)
    ) AS report_date,
  -- their first week days of use, taken from their 7th day data
    ANY_VALUE(
      CASE
        WHEN submission_date = DATE_ADD(first_seen_date, INTERVAL 6 DAY)
          THEN BIT_COUNT(days_visited_1_uri_bits & days_interacted_bits)
      END
    ) AS dou,
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
    `moz-fx-data-shared-prod.telemetry.clients_last_seen`
  WHERE
    submission_date >= first_cohort_date --greater than 11/1/2023
    AND submission_date
    BETWEEN first_seen_date
    AND DATE_ADD(first_seen_date, INTERVAL 6 DAY)
  GROUP BY
    client_id
),
combined AS (
  SELECT
    client_id,
    -- we should use their first_seen_date from clients_first_seen as their cohort date
    cfs.first_seen_date,
    cfs.attribution_campaign,
    cfs.attribution_content,
    cfs.attribution_dltoken,
    cfs.attribution_medium,
    cfs.attribution_source,
    --Report Date
    --If a client never sends a main ping after 27 days, set all conversion events to false
    --If a client does send a main ping before or equal to 27 days
    --if there is a report date, use that
    --
    COALESCE(
      cls.report_date,
      IF(
        (cls.report_date IS NULL)
        AND (cls.first_seen_date IS NULL)
        AND (DATE_DIFF(CURRENT_DATE, cfs.first_seen_date, DAY) >= 27),
        DATE_ADD(cfs.first_seen_date, INTERVAL 27 DAY),
        NULL
      ) AS report_date,
    -- not a strictly necessary field, used to see how many clients actually send a main ping
      IF(cls.first_seen_date IS NOT NULL, TRUE, FALSE) AS sent_main_ping_in_first_7_days,
    -- because the conversion events and ltv are based on their first observed country in CLS, use that country if its available.
      COALESCE(cls.country, cfs.country) AS country,
      COALESCE(dou, 0) AS dou,
      COALESCE(active_hours_sum, 0) AS active_hours_sum,
      COALESCE(search_with_ads_count_all, 0) AS search_with_ads_count_all
      FROM
        cfs
      LEFT JOIN
        cls
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
    report_date,
    sent_main_ping_in_first_7_days,
    country,
    dou,
    active_hours_sum,
    search_with_ads_count_all,
  --"strictest" event
    IF(report_date IS NOT NULL, (search_with_ads_count_all > 0) AND (dou >= 5), NULL) AS event_1,
  -- "medium" event
    IF(report_date IS NOT NULL, (search_with_ads_count_all > 0) AND (dou >= 3), NULL) AS event_2,
  -- "most_lenient" event
    IF(report_date IS NOT NULL, (active_hours_sum >= 0.4) AND (dou >= 3), NULL) AS event_3,
  FROM
    combined
