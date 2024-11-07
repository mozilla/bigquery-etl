{{ header }} {% raw %}
{% set _WHERE = 'metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)' %}

#warn
{{ min_row_count(1, where=_WHERE) }}

#warn
{{ not_null(["first_seen_date", "app_name", "normalized_channel", "is_mobile"], where=_WHERE) }}

#warn
{{ value_length(column="country", expected_length="2", where=_WHERE) }}

#warn
{{ accepted_values(column="normalized_channel", values=["release", "beta", "nightly"], where=_WHERE) }} {% endraw %}
