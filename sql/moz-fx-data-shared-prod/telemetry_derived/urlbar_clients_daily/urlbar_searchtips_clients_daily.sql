WITH
  searchtips_clients_daily_temp AS (
  SELECT
    submission_date,
    client_id,
    urlbar_tips.key AS tip,
    urlbar_tips.value AS n_engagements
  FROM
    `moz-fx-data-bq-data-science.teon.temp_urlbar_clients_daily`
  CROSS JOIN
    UNNEST(urlbar_tips) AS urlbar_tips),
  -- searchtips impressions
  searchtips_impression_clients_daily AS (
  SELECT
    submission_date,
    client_id,
    STRUCT(ARRAY_AGG(tip) AS key,
      ARRAY_AGG(n_engagements) AS value) AS searchtip_impression_count_by_type,
    COALESCE(SUM(n_engagements), 0) AS searchtip_impression_count
  FROM
    searchtips_clients_daily_temp
  WHERE
    tip LIKE '%shown'
  GROUP BY
    submission_date,
    client_id),
  -- searchtips engagements
  searchtips_engagement_clients_daily AS (
  SELECT
    submission_date,
    client_id,
    STRUCT(ARRAY_AGG(tip) AS key,
      ARRAY_AGG(n_engagements) AS value) AS searchtip_engagement_count_by_type,
    COALESCE(SUM(n_engagements), 0) AS searchtip_engagement_count
  FROM
    searchtips_clients_daily_temp
  WHERE
    tip LIKE '%picked'
  GROUP BY
    client_id,
    submission_date)
SELECT
  *
FROM
  searchtips_impression_clients_daily
FULL OUTER JOIN
  searchtips_engagement_clients_daily
USING
  (submission_date,
    client_id)
