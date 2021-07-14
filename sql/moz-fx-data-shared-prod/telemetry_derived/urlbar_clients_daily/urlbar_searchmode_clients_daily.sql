WITH
  searchmode_clients_daily_temp AS (
  SELECT
    submission_date,
    client_id,
    probe_name,
    probe_searchmode.key AS searchmode_engine,
    probe_searchmode.value AS n_engagements
  FROM
    `moz-fx-data-bq-data-science.teon.temp_urlbar_clients_daily`
  CROSS JOIN
    UNNEST([ STRUCT("bookmarkmenu" AS probe_name,
        searchmode_bookmarkmenu AS probe), STRUCT("handoff" AS probe_name,
        searchmode_handoff AS probe), STRUCT("keywordoffer" AS probe_name,
        searchmode_keywordoffer AS probe), STRUCT("oneoff" AS probe_name,
        searchmode_oneoff AS probe), STRUCT("other" AS probe_name,
        searchmode_other AS probe), STRUCT("shortcut" AS probe_name,
        searchmode_shortcut AS probe), STRUCT("tabmenu" AS probe_name,
        searchmode_tabmenu AS probe), STRUCT("tabtosearch" AS probe_name,
        searchmode_tabtosearch AS probe), STRUCT("tabtosearch_onboard" AS probe_name,
        searchmode_tabtosearch_onboard AS probe), STRUCT("topsites_newtab" AS probe_name,
        searchmode_topsites_newtab AS probe), STRUCT("topsites_urlbar" AS probe_name,
        searchmode_topsites_urlbar AS probe), STRUCT("touchbar" AS probe_name,
        searchmode_touchbar AS probe), STRUCT("typed" AS probe_name,
        searchmode_typed AS probe)])
  CROSS JOIN
    UNNEST(probe) AS probe_searchmode ),
  searchmode_clients_daily_by_entry_temp AS (
  SELECT
    submission_date,
    client_id,
    probe_name,
    SUM(n_engagements) AS n_engagements
  FROM
    searchmode_clients_daily_temp
  GROUP BY
    submission_date,
    client_id,
    probe_name ),
  searchmode_clients_daily_by_entry AS (
  SELECT
    submission_date,
    client_id,
    STRUCT(ARRAY_AGG(probe_name) AS key,
      ARRAY_AGG(n_engagements) AS value) AS searchmode_by_entry
  FROM
    searchmode_clients_daily_by_entry_temp
  GROUP BY
    submission_date,
    client_id),
  searchmode_clients_daily_by_engine_temp AS (
  SELECT
    submission_date,
    client_id,
    searchmode_engine,
    SUM(n_engagements) AS n_engagements
  FROM
    searchmode_clients_daily_temp
  GROUP BY
    submission_date,
    client_id,
    searchmode_engine ),
  searchmode_clients_daily_by_engine AS (
  SELECT
    submission_date,
    client_id,
    STRUCT(ARRAY_AGG(searchmode_engine) AS key,
      ARRAY_AGG(n_engagements) AS value) AS searchmode_by_engine
  FROM
    searchmode_clients_daily_by_engine_temp
  GROUP BY
    submission_date,
    client_id)
SELECT
  *
FROM
  searchmode_clients_daily_by_entry
INNER JOIN
  searchmode_clients_daily_by_engine
USING
  (submission_date,
    client_id)