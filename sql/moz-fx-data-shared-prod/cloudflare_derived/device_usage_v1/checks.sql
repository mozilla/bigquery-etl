#warn
{{ is_unique(["dte", "location", "user_type"], "dte = DATE_SUB(@dte, INTERVAL 4 DAY)") }}
