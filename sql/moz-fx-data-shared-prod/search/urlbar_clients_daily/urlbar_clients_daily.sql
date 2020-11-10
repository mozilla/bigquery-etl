  -- build it all up
SELECT
  *
FROM
  `moz-fx-data-bq-data-science.teon.urlbar_event_clients_daily`
FULL OUTER JOIN
  `moz-fx-data-bq-data-science.teon.urlbar_searchmode_clients_daily`
USING
  (submission_date,
    client_id)
FULL OUTER JOIN
  `moz-fx-data-bq-data-science.teon.urlbar_picked_clients_daily`
USING
  (submission_date,
    client_id)
FULL OUTER JOIN
  `moz-fx-data-bq-data-science.teon.urlbar_searchtip_clients_daily`
USING
  (submission_date,
    client_id)