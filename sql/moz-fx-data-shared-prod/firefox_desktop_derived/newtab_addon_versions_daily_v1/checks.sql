-- Grain: one row per client per newtab addon version per day.

#fail
{{ is_unique(["submission_date", "client_id", "newtab_addon_version"], "submission_date = @submission_date") }}
--
--
-- Join safety: at most one last-reported version per client per day, so joins to
-- newtab_visits_daily filtered to is_last_reported_version don't double-count visits.

#fail
{{ is_unique(["submission_date", "client_id"], "submission_date = @submission_date AND is_last_reported_version") }}
