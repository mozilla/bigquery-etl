#warn
{{ is_unique(["country", "app_name", "adjust_network","attribution_medium","attribution_source", "first_seen_year","channel","install_source","is_default_browser","os_grouped","segment"], "submission_date = @submission_date") }}
