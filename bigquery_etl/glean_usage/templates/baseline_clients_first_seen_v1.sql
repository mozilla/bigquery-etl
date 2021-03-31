{{ header }}

{% if init %}

CREATE TABLE IF NOT EXISTS
  `{{ first_seen_table }}`
PARTITION BY
  submission_date
CLUSTER BY
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  DATE(submission_timestamp) as submission_date,
  sample_id,
  client_info.client_id
FROM
  `{{ baseline_table }}`
WHERE
  FALSE

{% else %}

WITH _current AS (
  SELECT
    sample_id,
    client_info.client_id
  FROM
    `{{ baseline_table }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
  --
_previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    `{{ first_seen_table }}`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
)
  --
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).*
FROM
  _current
FULL JOIN
  _previous
USING
  (client_id)

{% endif %}
