  -- urlbar event count
WITH
  urlbar_event_clients_daily_temp AS (
  SELECT
    submission_date,
    client_id,
    events.key AS event_name,
    events.value AS event_count
  FROM
    `moz-fx-data-bq-data-science.teon.temp_urlbar_clients_daily`
  CROSS JOIN
    UNNEST(urlbar_event_counts) AS events )
  -- urlbar_event_counts
SELECT
  submission_date,
  client_id,
  STRUCT(ARRAY_AGG(event_name) AS key,
    ARRAY_AGG(event_count) AS value) AS urlbar_event
FROM
  urlbar_event_clients_daily_temp
WHERE
  event_name LIKE 'urlbar#%#%'
GROUP BY
  submission_date,
  client_id