{{ header }}

{% raw %}
{% if is_init() %}
{% endraw %}

SELECT
  CAST(NULL AS INT64) AS days_seen_bits,
  CAST(NULL AS INT64) AS days_active_bits,
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

{% raw %}
{% else %}
{% endraw %}

WITH _current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit in 'days_since_seen' represents whether the user sent a
    -- baseline ping in the submission_date and similarly, the rightmost bit in
    -- days_active_bits represents whether the user counts as active on that date.
    CAST(TRUE AS INT64) AS days_seen_bits,
    CAST(TRUE AS INT64) & CAST(durations > 0  AS INT64) AS days_active_bits,
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
    days_seen_bits, days_active_bits, days_created_profile_bits,
    * EXCEPT (submission_date, days_seen_bits, days_active_bits, days_created_profile_bits),
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

{% raw %}
{% endif %}
{% endraw %}
