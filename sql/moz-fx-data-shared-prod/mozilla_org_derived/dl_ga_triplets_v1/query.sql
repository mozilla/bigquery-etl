WITH historical_triplets AS (
  SELECT
    dl_token,
    ga_client_id,
    stub_session_id,
    first_seen_date,
  FROM
    mozdata.analysis.dl_ga_triplets_v1
),
new_downloads AS (
  SELECT
    mozdata.analysis.ga_nullify_string(jsonPayload.fields.dltoken) AS dl_token,
    mozdata.analysis.ga_nullify_string(jsonPayload.fields.visit_id) AS ga_client_id,
    mozdata.analysis.ga_nullify_string(jsonPayload.fields.session_id) AS stub_session_id,
    @download_date AS first_seen_date,
    -- Stub attribution logs have a 90-day retention period, so we don't want to overwrite our data if it's been deleted from the source!
    IF(
      COUNT(*) OVER () < 100,
      ERROR(
        "Error: Stub attribution logs have already been deleted for date " || CAST(
          @download_date AS STRING
        )
      ),
      TRUE
    ) AS valid,
  FROM
    `moz-fx-stubattribut-prod-32a5`.stubattribution_prod.stdout
  WHERE
    DATE(timestamp) = @download_date
)
SELECT
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
