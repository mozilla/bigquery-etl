-- Pipeline-health canary: fail if no rows were written for the partition,
-- which signals upstream newtab_v1 breakage or that widget telemetry stopped emitting.

#fail
{{ min_row_count(1, "submission_date = @submission_date") }}
--
--
-- Grain integrity: enforce the documented "one row per (submission_date, widget_name)"
-- grain so downstream aggregations stay correct if the GROUP BY ever changes.

#fail
{{ is_unique(["submission_date", "app_version", "os", "channel", "country", "locale", "widget_name"], "submission_date = @submission_date") }}
--
--
-- Defensive guard: query.sql filters out null widget_name, so this should never fire;
-- warn if it does so a regression in the WHERE clause is surfaced without blocking downstream.

#warn
{{ not_null(["widget_name"], "submission_date = @submission_date") }}
