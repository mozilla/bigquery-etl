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
    * EXCEPT (submission_date, registered, seen_in_tier1_country),
  FROM
    `moz-fx-data-shared-prod.accounts_backend_derived.users_services_daily_v1`
  WHERE
    submission_date = @submission_date
),
_previous AS (
  SELECT
    days_seen_bits,
    days_seen_in_tier1_country_bits,
    days_registered_bits,
    * EXCEPT (
      days_seen_bits,
      days_seen_in_tier1_country_bits,
      days_registered_bits,
      submission_date
    )
  FROM
    `moz-fx-data-shared-prod.accounts_backend_derived.users_services_last_seen_v1`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND `moz-fx-data-shared-prod.udf.shift_28_bits_one_day`(days_seen_bits) > 0
)
SELECT
  @submission_date AS submission_date,
  IF(_current.user_id_sha256 IS NOT NULL, _current, _previous).* REPLACE (
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_28_bits`(
      _previous.days_seen_bits,
      _current.days_seen_bits
    ) AS days_seen_bits,
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_28_bits`(
      _previous.days_seen_in_tier1_country_bits,
      _current.days_seen_in_tier1_country_bits
    ) AS days_seen_in_tier1_country_bits,
    `moz-fx-data-shared-prod.udf.coalesce_adjacent_days_28_bits`(
      _previous.days_registered_bits,
      _current.days_registered_bits
    ) AS days_registered_bits
  )
FROM
  _current
FULL JOIN
  _previous
  USING (user_id_sha256, service)
