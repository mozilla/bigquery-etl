#warn
{{ is_unique(["client_id"], "first_seen_date <= current_date") }}
