-- Every column in the two checks below is a FULL OUTER JOIN key in query.sql.
-- NULL never matches NULL in a join, so a null key leaves the displays half and
-- the actions half of one group as two separate rows, which the uniqueness check
-- reads as a duplicate. The query coalesces each key to '??'; not_null holds that
-- line.

#fail
{{ not_null(["submission_date", "normalized_channel", "normalized_os", "country", "app_version_major", "threat_type", "scope"], "submission_date = @submission_date") }}

#fail
{{ is_unique(["submission_date", "normalized_channel", "normalized_os", "country", "app_version_major", "threat_type", "scope"], "submission_date = @submission_date") }}

#warn
{{ min_row_count(1, "submission_date = @submission_date") }}
-- Drift alarm for the event-code and category-label mappings.
--
-- threat_type and scope are transcribed from two source files:
-- IUrlClassifierUITelemetry.idl (event codes 1-32) and
-- nsDocShellTelemetryUtils.cpp (page.load_error category labels). Both decodes
-- fall through to NULL rather than mislabelling if a new value appears. The
-- not_null check above turns that into a failure so the mapping gets updated
-- instead of the table quietly under-reporting.
--
-- Actions without a matching display are the signal that the join keys have
-- drifted apart, for example if a category label is renamed on one side only.
--
-- Two reasons this is a rate and not a count of zero.
--
-- First, Firefox 141 and older record the ui_events actions but not
-- page.load_error, so every one of their rows has actions with zero displays.
-- That is the instrumentation, not drift, and they are excluded. The same
-- exclusion applies to the two ratio checks below, which need both halves of
-- the funnel to mean anything.
--
-- Second, a few orphans survive on 142 and newer. The two counters flush
-- independently, so a user who sees the warning just before a ping is sent and
-- presses the button after it lands in a partition with no matching display.
-- Measured on 142+ over 2026-09-01 to 2026-09-14: 0 to 10 orphaned presses a
-- day, never above 0.17% of all presses. The 1% bound leaves room for that and
-- still catches a renamed label, which would orphan a whole threat type or
-- scope at once, far above 1%.

#warn
ASSERT (
  SELECT
    IFNULL(
      SAFE_DIVIDE(
        SUM(IF(displays = 0, left_site + proceeded_anyway, 0)),
        SUM(left_site + proceeded_anyway)
      ),
      0
    )
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND app_version_major >= 142
) <= 0.01
AS
  "More than 1% of interstitial button presses on Firefox 142 or newer have no matching display for the same date, channel, OS, country, Firefox major version, threat type and scope. The page.load_error category labels and the ui_events codes may have drifted apart.";

-- The reload inflation should stay in its observed range.
--
-- Displays count renders, not unique encounters, because blocked URLs are never
-- cache-tagged. Measured 1.7-2.1 displays per client in July and August 2026.
-- Below 1.0 is impossible and means the client count is wrong. A jump well above
-- the observed range would change how much the bypass rate is understated, and
-- the caveat in the docs would need revisiting.

#warn
ASSERT (
  SELECT
    SAFE_DIVIDE(SUM(displays), SUM(display_clients))
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND app_version_major >= 142
    AND normalized_channel = 'release'
    AND scope = 'Top-level page'
)
BETWEEN 1.0
AND 4.0
AS
  "Displays per client on release is outside the observed 1.7-2.1 range (bounds 1.0-4.0). Either the reload inflation has changed or the client counts are wrong.";

-- Sanity bound on the override rate.
--
-- proceeded_anyway / displays for top-level phishing on release, counting only
-- Firefox 142 and newer, measured 18% on 2026-08-31 and 13% on 2026-09-14. The
-- earlier ~23% figure in this file included Firefox 141 and older, whose presses
-- have no displays to divide by and so inflated the numerator.
-- This is a LOWER BOUND, not a point estimate, because the
-- denominator is inflated by reloads. Wide bounds here on purpose: this catches a
-- decode regression that swaps the two actions, not a subtle behavioural shift.

#warn
ASSERT (
  SELECT
    SAFE_DIVIDE(SUM(proceeded_anyway), SUM(displays))
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND app_version_major >= 142
    AND normalized_channel = 'release'
    AND scope = 'Top-level page'
    AND threat_type = 'Phishing'
)
BETWEEN 0.05
AND 0.6
AS
  "Top-level phishing override rate on release is outside its historical 5-60% band; check for a behavioural shift or a swapped action mapping.";
