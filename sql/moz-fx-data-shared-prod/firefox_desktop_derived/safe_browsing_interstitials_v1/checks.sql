#fail
{{ not_null(["submission_date", "threat_type", "scope"], "submission_date = @submission_date") }}

#fail
{{ is_unique(["submission_date", "normalized_channel", "normalized_os", "country", "threat_type", "scope"], "submission_date = @submission_date") }}

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

#warn
ASSERT (
  SELECT
    COUNTIF(displays = 0 AND (left_site > 0 OR proceeded_anyway > 0))
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
) = 0
AS
  "Interstitial actions recorded with zero displays for the same date, channel, OS, country, threat type and scope. The page.load_error category labels and the ui_events codes may have drifted apart.";

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
    AND normalized_channel = 'release'
    AND scope = 'Top-level page'
)
BETWEEN 1.0
AND 4.0
AS
  "Displays per client on release is outside the observed 1.7-2.1 range (bounds 1.0-4.0). Either the reload inflation has changed or the client counts are wrong.";

-- Sanity bound on the override rate.
--
-- proceeded_anyway / displays for top-level phishing on release measured ~23% in
-- August 2026. This is a LOWER BOUND, not a point estimate, because the
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
    AND normalized_channel = 'release'
    AND scope = 'Top-level page'
    AND threat_type = 'Phishing'
)
BETWEEN 0.05
AND 0.6
AS
  "Top-level phishing override rate on release is outside its historical 5-60% band; check for a behavioural shift or a swapped action mapping.";
