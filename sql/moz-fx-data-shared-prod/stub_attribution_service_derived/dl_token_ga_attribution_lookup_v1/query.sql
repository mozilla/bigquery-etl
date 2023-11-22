WITH historical_triplets AS (
  SELECT
    IFNULL(dl_token, "") AS dl_token,
    IFNULL(ga_client_id, "") AS ga_client_id,
    IFNULL(stub_session_id, "") AS stub_session_id,
    first_seen_date,
  FROM
    stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1
),
new_downloads AS (
  SELECT DISTINCT
    IFNULL(mozfun.ga.nullify_string(jsonPayload.fields.dltoken), "") AS dl_token,
    IFNULL(mozfun.ga.nullify_string(jsonPayload.fields.visit_id), "") AS ga_client_id,
    IFNULL(mozfun.ga.nullify_string(jsonPayload.fields.session_id), "") AS stub_session_id,
    @download_date AS first_seen_date,
  FROM
    `moz-fx-stubattribut-prod-32a5`.stubattribution_prod.stdout
  WHERE
    DATE(timestamp) = @download_date
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
USING
  (dl_token, ga_client_id, stub_session_id)
