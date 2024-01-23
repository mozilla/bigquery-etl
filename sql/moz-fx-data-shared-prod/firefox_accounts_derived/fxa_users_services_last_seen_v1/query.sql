WITH _current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit represents whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_seen_bits,
    -- Record days on which the user was in a "Tier 1" country;
    -- this allows a variant of country-segmented MAU where we can still count
    -- a user that appeared in one of the target countries in the previous
    -- 28 days even if the most recent "country" value is not in this set.
    CAST(seen_in_tier1_country AS INT64) AS days_seen_in_tier1_country_bits,
    CAST(registered AS INT64) AS days_registered_bits,
    * EXCEPT (submission_date, seen_in_tier1_country, registered)
  FROM
    fxa_users_services_daily_v1
  WHERE
    submission_date = @submission_date
),
  --
_previous AS (
  SELECT
    * EXCEPT (submission_date, resurrected_same_service, resurrected_any_service)
  FROM
    fxa_users_services_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
),
  --
combined AS (
  SELECT
    @submission_date AS submission_date,
    IF(_current.user_id IS NOT NULL, _current, _previous).* REPLACE ( --
      udf.combine_adjacent_days_28_bits(
        _previous.days_seen_bits,
        _current.days_seen_bits
      ) AS days_seen_bits,
      udf.combine_adjacent_days_28_bits(
        _previous.days_seen_in_tier1_country_bits,
        _current.days_seen_in_tier1_country_bits
      ) AS days_seen_in_tier1_country_bits,
      udf.coalesce_adjacent_days_28_bits(
        _previous.days_registered_bits,
        _current.days_registered_bits
      ) AS days_registered_bits
    ),
    _previous.user_id IS NULL AS new_observation -- used for determining "resurrected" criteria
  FROM
    _current
  FULL JOIN
    _previous
    USING (user_id, service)
),
  --
previously_seen AS (
  SELECT
    user_id,
    service
  FROM
    fxa_users_services_first_seen_v1
  WHERE
    DATE(first_service_timestamp) < @submission_date
),
  --
previously_seen_users AS (
  SELECT DISTINCT
    user_id
  FROM
    previously_seen
)
SELECT
  combined.* EXCEPT (new_observation),
  -- Note that the first_seen table only contains data from 2019-03-01 on, so
  -- we have to accept that users who joined before that date will not show up
  -- as resurrected when they first return.
  (new_observation AND same_service.user_id IS NOT NULL) resurrected_same_service,
  (new_observation AND any_service.user_id IS NOT NULL) resurrected_any_service
FROM
  combined
LEFT JOIN
  previously_seen AS same_service
  USING (user_id, service)
LEFT JOIN
  previously_seen_users AS any_service
  USING (user_id)
