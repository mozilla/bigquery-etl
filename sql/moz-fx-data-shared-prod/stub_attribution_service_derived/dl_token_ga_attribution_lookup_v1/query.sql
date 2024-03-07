--Note: This table `moz-fx-stubattribut-prod-32a5`.stubattribution_prod.stdout did not always save full history
--Backfills should NOT be done on this table prior to 2023-08-24
WITH check_download_date AS (
  SELECT
    IF(
      @download_date < "2023-08-25",
      ERROR("Cannot backfill date before 2023-08-25"),
      CAST(@download_date AS string)
    ) AS download_date
),
historical_triplets AS (
  SELECT
    IFNULL(dl_token, "") AS dl_token,
    IFNULL(ga_client_id, "") AS ga_client_id,
    IFNULL(stub_session_id, "") AS stub_session_id,
    first_seen_date,
  FROM
    `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1`
),
new_downloads_stg AS (
  -- Prior to GA4, use visit_id as the GA3 client ID
  SELECT DISTINCT
    IFNULL(mozfun.ga.nullify_string(jsonPayload.fields.dltoken), "") AS dl_token,
    IFNULL(mozfun.ga.nullify_string(jsonPayload.fields.visit_id), "") AS ga_client_id,
    IFNULL(mozfun.ga.nullify_string(jsonPayload.fields.session_id), "") AS stub_session_id,
    CAST(@download_date AS date) AS first_seen_date,
  FROM
    `moz-fx-stubattribut-prod-32a5.stubattribution_prod.stdout`
  WHERE
    DATE(timestamp) = (SELECT CAST(download_date AS date) FROM check_download_date)
    AND timestamp < '2024-03-05 21:49:42.355439 UTC' --when the GA4 client ID column started getting data
  UNION ALL
  --Post GA4 launch, use client_id_ga4 as the GA4 client ID
  SELECT DISTINCT
    IFNULL(mozfun.ga.nullify_string(jsonPayload.fields.dltoken), "") AS dl_token,
    IFNULL(mozfun.ga.nullify_string(jsonPayload.fields.client_id_ga4), "") AS ga_client_id,
    IFNULL(mozfun.ga.nullify_string(jsonPayload.fields.session_id), "") AS stub_session_id,
    CAST(@download_date AS date) AS first_seen_date,
  FROM
    `moz-fx-stubattribut-prod-32a5.stubattribution_prod.stdout`
  WHERE
    DATE(timestamp) = (SELECT CAST(download_date AS date) FROM check_download_date)
    AND timestamp >= '2024-03-05 21:49:42.355439 UTC' --when the GA4 client ID column started getting data
),
new_downloads AS (
  SELECT
    dl_token,
    ga_client_id,
    stub_session_id,
    MIN(first_seen_date) AS first_seen_date
  FROM
    new_downloads_stg
  GROUP BY
    dl_token,
    ga_client_id,
    stub_session_id
)
SELECT
  -- We can't store these as NULL, since joins on NULL keys don't match.
  -- Since they don't match, we end up with dupes
  dl_token,
  ga_client_id,
  stub_session_id,
  -- Least and greatest return NULL if any input is NULL, so we coalesce each value first
  LEAST(
    COALESCE(_previous.first_seen_date, _current.first_seen_date),
    COALESCE(_current.first_seen_date, _previous.first_seen_date)
  ) AS first_seen_date,
FROM
  historical_triplets AS _previous
FULL OUTER JOIN
  new_downloads AS _current
  USING (dl_token, ga_client_id, stub_session_id)
