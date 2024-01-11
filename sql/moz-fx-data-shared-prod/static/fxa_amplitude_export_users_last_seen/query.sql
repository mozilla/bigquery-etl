WITH current_sample AS (
  SELECT
    -- Record the last day on which we received any FxA event at all from this user.
    @submission_date AS date_last_seen,
    -- Record the last day on which the user was in a "Tier 1" country;
    -- this allows a variant of country-segmented MAU where we can still count
    -- a user that appeared in one of the target countries in the previous
    -- 28 days even if the most recent "country" value is not in this set.
    IF(seen_in_tier1_country, @submission_date, NULL) AS date_last_seen_in_tier1_country,
    * EXCEPT (submission_date, seen_in_tier1_country)
  FROM
    fxa_amplitude_export_users_daily
  WHERE
    submission_date = @submission_date
),
previous AS (
  SELECT
    * EXCEPT (submission_date)
      -- We use REPLACE to null out any last_seen observations older than 28 days;
      -- this ensures data never bleeds in from outside the target 28 day window.
    REPLACE(
      IF(
        date_last_seen_in_tier1_country > DATE_SUB(@submission_date, INTERVAL 28 DAY),
        date_last_seen_in_tier1_country,
        NULL
      ) AS date_last_seen_in_tier1_country
    )
  FROM
    fxa_amplitude_export_users_last_seen
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND date_last_seen > DATE_SUB(@submission_date, INTERVAL 28 DAY)
)
SELECT
  @submission_date AS submission_date,
  COALESCE(current_sample.date_last_seen, previous.date_last_seen) AS date_last_seen,
  COALESCE(
    current_sample.date_last_seen_in_tier1_country,
    previous.date_last_seen_in_tier1_country
  ) AS date_last_seen_in_tier1_country,
  IF(current_sample.user_id IS NOT NULL, current_sample, previous).* EXCEPT (
    date_last_seen,
    date_last_seen_in_tier1_country
  )
FROM
  current_sample
FULL JOIN
  previous
  USING (user_id)
