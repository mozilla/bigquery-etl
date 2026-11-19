#fail
{{ min_row_count(1, "date = @submission_date") }}
-- pathname and country_code are deliberately not checked not-null: Plausible
-- normalizes empty strings to NULL uniformly across every string column
-- (observed at 72-79% empty for other fields like referrer/subdivision2_code
-- in plausible_external/README.md), so a NULL country_code from an
-- unresolvable IP geolocation (VPN, bot, private IP) is expected data, not a
-- pipeline failure -- it should not block this DAG.

#fail
{{ not_null(["date", "event_name"], "date = @submission_date") }}

#fail
{{ is_unique(["date", "pathname", "country_code", "acquisition_channel", "utm_campaign", "event_name"], "date = @submission_date") }}
