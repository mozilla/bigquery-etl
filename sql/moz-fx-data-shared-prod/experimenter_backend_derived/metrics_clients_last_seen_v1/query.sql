WITH _previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    `moz-fx-data-shared-prod.experimenter_backend_derived.metrics_clients_last_seen_v1`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND `moz-fx-data-shared-prod.udf.shift_28_bits_one_day`(days_sent_metrics_ping_bits) > 0
),
_current AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.experimenter_backend.metrics_clients_daily` AS m
  WHERE
    submission_date = @submission_date
)
SELECT
  DATE(@submission_date) AS submission_date,
  _current.client_id,
  _current.sample_id,
  _current.normalized_channel,
  _current.n_metrics_ping,
  `moz-fx-data-shared-prod.udf.combine_adjacent_days_28_bits`(
    _previous.days_sent_metrics_ping_bits,
    _current.days_sent_metrics_ping_bits
  ) AS days_sent_metrics_ping_bits,
FROM
  _previous
FULL JOIN
  _current
  ON _previous.client_id = _current.client_id
  AND _previous.sample_id = _current.sample_id
  AND (
    _previous.normalized_channel = _current.normalized_channel
    OR (_previous.normalized_channel IS NULL AND _current.normalized_channel IS NULL)
  )
WHERE
  _current.client_id IS NOT NULL
