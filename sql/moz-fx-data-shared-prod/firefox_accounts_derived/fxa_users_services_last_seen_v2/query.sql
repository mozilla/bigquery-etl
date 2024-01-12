-- TODO: for now this is a copy of v1, we still need to adjust and make sure
-- no other changes are required. Will be done in a separate change.
--
-- contains_qualified_fxa_activity_event
-- Expects an array of event_types and service name
-- Iterates and checks if an event_type, service combination
-- should be considered when determining if this is the first
-- time it has been observed.
-- Example usage:
-- contains_qualified_fxa_activity_event(['fxa_activity'], NULL),  # returns TRUE
-- contains_qualified_fxa_activity_event(['fxa_activity'], 'firefox-ios'),  # returns TRUE
-- contains_qualified_fxa_activity_event(['fxa_login - complete'], NULL),  # returns FALSE
-- contains_qualified_fxa_activity_event(['fxa_login - complete'], 'firefox-ios'),  # returns TRUE
CREATE TEMP FUNCTION contains_qualified_fxa_activity_event(events ANY TYPE) AS (
  EXISTS(
    SELECT
      event_type
    FROM
      UNNEST(events) AS event_type
    WHERE
      event_type IN ('fxa_login - complete', 'fxa_reg - complete')
      OR (event_type LIKE r'fxa\_activity%')
  )
);

WITH _current AS (
  SELECT
    * EXCEPT (
      submission_date,
      ua_version,
      ua_browser,
      seen_in_tier1_country,
      registered,
      service_events,
      user_service_first_daily_flow_info,
      user_service_utm_info
    ),
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
  FROM
    firefox_accounts_derived.fxa_users_services_daily_v2
  WHERE
    submission_date = @submission_date
    AND contains_qualified_fxa_activity_event(service_events)
),
_previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    firefox_accounts_derived.fxa_users_services_last_seen_v2
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
)
SELECT
  @submission_date AS submission_date,
  IF(_current.user_id IS NOT NULL, _current, _previous).* REPLACE (
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
  )
FROM
  _current
FULL JOIN
  _previous
  USING (user_id, service)
