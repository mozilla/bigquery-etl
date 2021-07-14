  -- urlbar picked type count
WITH
  urlbar_picked_clients_daily_temp AS (
  SELECT
    submission_date,
    client_id,
    result_type,
    result.key AS result_index,
    result.value AS n_engagements
  FROM
    `moz-fx-data-bq-data-science.teon.temp_urlbar_clients_daily`
  CROSS JOIN
    UNNEST ([ STRUCT("urlbar_autofill" AS result_type,
        urlbar_autofill AS probe), STRUCT("urlbar_bookmark" AS result_type,
        urlbar_bookmark AS probe), STRUCT("urlbar_dynamic" AS result_type,
        urlbar_dynamic AS probe), STRUCT("urlbar_extension" AS result_type,
        urlbar_extension AS probe), STRUCT("urlbar_formhistory" AS result_type,
        urlbar_formhistory AS probe), STRUCT("urlbar_history" AS result_type,
        urlbar_history AS probe), STRUCT("urlbar_keyword" AS result_type,
        urlbar_keyword AS probe), STRUCT("urlbar_remotetab" AS result_type,
        urlbar_remotetab AS probe), STRUCT("urlbar_searchengine" AS result_type,
        urlbar_searchengine AS probe), STRUCT("urlbar_searchsuggestion" AS result_type,
        urlbar_searchsuggestion AS probe), STRUCT("urlbar_switchtab" AS result_type,
        urlbar_switchtab AS probe), STRUCT("urlbar_tabtosearch" AS result_type,
        urlbar_tabtosearch AS probe), STRUCT("urlbar_tip" AS result_type,
        urlbar_tip AS probe), STRUCT("urlbar_topsite" AS result_type,
        urlbar_topsite AS probe), STRUCT("urlbar_unknown" AS result_type,
        urlbar_unknown AS probe), STRUCT("urlbar_visiturl" AS result_type,
        urlbar_visiturl AS probe) ])
  CROSS JOIN
    UNNEST(probe) AS result),
  urlbar_picked_clients_daily_by_type_temp AS (
  SELECT
    submission_date,
    client_id,
    result_type,
    SUM(n_engagements) AS n_engagements
  FROM
    urlbar_picked_clients_daily_temp
  GROUP BY
    submission_date,
    client_id,
    result_type ),
  urlbar_picked_clients_daily_by_type AS (
  SELECT
    client_id,
    submission_date,
    STRUCT(ARRAY_AGG(result_type) AS key,
      ARRAY_AGG(n_engagements) AS value) AS urlbar_picked_by_type
  FROM
    urlbar_picked_clients_daily_by_type_temp
  GROUP BY
    submission_date,
    client_id ),
  urlbar_picked_clients_daily_by_index_temp AS (
  SELECT
    submission_date,
    client_id,
    result_index,
    SUM(n_engagements) AS n_engagements
  FROM
    urlbar_picked_clients_daily_temp
  GROUP BY
    submission_date,
    client_id,
    result_index ),
  urlbar_picked_clients_daily_by_index AS (
  SELECT
    submission_date,
    client_id,
    STRUCT(ARRAY_AGG(result_index) AS key,
      ARRAY_AGG(n_engagements) AS value) AS urlbar_picked_by_index
  FROM
    urlbar_picked_clients_daily_by_index_temp
  GROUP BY
    submission_date,
    client_id )
SELECT
  *
FROM
  urlbar_picked_clients_daily_by_index
FULL OUTER JOIN
  urlbar_picked_clients_daily_by_type
USING
  (submission_date,
    client_id)