-- Generated via bigquery_etl.glean_usage
{% if is_init() %}
  WITH baseline AS (
    SELECT
      client_info.client_id,
      -- Some Glean data from 2019 contains incorrect sample_id, so we
      -- recalculate here; see bug 1707640
      `moz-fx-data-shared-prod.udf.safe_sample_id`(client_info.client_id) AS sample_id,
      DATE(MIN(submission_timestamp)) AS submission_date,
      DATE(MIN(submission_timestamp)) AS first_seen_date,
    FROM
      `moz-fx-data-shared-prod.net_thunderbird_android_beta_stable.baseline_v1`
    -- initialize by looking over all of history
    WHERE
      DATE(submission_timestamp) > "2010-01-01"
    GROUP BY
      client_id,
      sample_id
  )
  SELECT
    *
  FROM
    baseline
{% else %}
  WITH _current AS (
    SELECT DISTINCT
      @submission_date AS submission_date,
      @submission_date AS first_seen_date,
      sample_id,
      client_info.client_id
    FROM
      `moz-fx-data-shared-prod.net_thunderbird_android_beta_stable.baseline_v1`
    WHERE
      DATE(submission_timestamp) = @submission_date
      AND client_info.client_id IS NOT NULL -- Bug 1896455
  ),
  -- query over all of history to see whether the client_id has shown up before
  _previous AS (
    SELECT
      submission_date,
      first_seen_date,
      sample_id,
      client_id
    FROM
      `moz-fx-data-shared-prod.net_thunderbird_android_beta_derived.baseline_clients_first_seen_v1`
    WHERE
      first_seen_date > "2010-01-01"
      AND first_seen_date < @submission_date
  ),
  _joined AS (
    SELECT
      IF(
        _previous.client_id IS NULL
        OR _previous.first_seen_date >= _current.first_seen_date,
        _current,
        _previous
      ).*
    FROM
      _current
    FULL JOIN
      _previous
      USING (client_id)
  )
-- added this as the result of bug#1788650
  SELECT
    submission_date,
    first_seen_date,
    sample_id,
    client_id
  FROM
    _joined
  QUALIFY
    IF(COUNT(*) OVER (PARTITION BY client_id) > 1, ERROR("duplicate client_id detected"), TRUE)
{% endif %}