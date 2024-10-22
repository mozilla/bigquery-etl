{{ header }} {% raw %}
{% set _WHERE = 'submission_date = @submission_date' %}

#warn
{{ min_row_count(1, where=_WHERE) }}

#warn
{{ is_unique(["client_id"], where=_WHERE)}}
{% endraw %}