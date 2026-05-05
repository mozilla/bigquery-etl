SELECT
  * EXCEPT (submission_date)
FROM
  {{ dataset }}_derived.event_types_history_v1
{% raw %}
{% if not is_init() %}
WHERE
  submission_date = @submission_date
{% endif %}
{% endraw %}
