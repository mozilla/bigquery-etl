{{ header }}

{% if init %}

CREATE TABLE IF NOT EXISTS
  `{{ last_seen_table }}`
PARTITION BY
  submission_date
CLUSTER BY
  normalized_channel,
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS INT64) AS days_seen_bits,
  CAST(NULL AS INT64) AS days_created_profile_bits,
  -- We make sure to delay * until the end so that as new columns are added
  -- to the daily table we can add those columns in the same order to the end
  -- of this schema, which may be necessary for the daily join query between
  -- the two tables to validate.
  *
FROM
  `{{ daily_table }}`
WHERE
  -- Output empty table and read no input rows
  FALSE

{% else %}

WITH _current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit represents whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_seen_bits,
    udf.days_since_created_profile_as_28_bits(
      DATE_DIFF(submission_date, first_run_date, DAY)
    ) AS days_created_profile_bits,
    * EXCEPT(submission_date)
  FROM
    `{{ daily_table }}`
  WHERE
    submission_date = @submission_date
    AND sample_id IS NOT NULL
),
  --
_previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    `{{ last_seen_table }}`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
    AND sample_id IS NOT NULL
)
  --
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    {% for ut in usage_types %}
      udf.combine_adjacent_days_28_bits(
        _previous.days_{{ ut }}_bits,
        _current.days_{{ ut }}_bits
      ) AS days_{{ ut }}_bits
      {{ "," if not loop.last }}
    {% endfor %}
  )
FROM
  _current
FULL JOIN
  _previous
  USING (client_id)

{% endif %}
