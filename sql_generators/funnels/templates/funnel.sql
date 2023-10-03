{% for funnel_name, funnel in funnels.items() %}
{% if loop.first %}WITH{% endif %}
{% for step_name in funnel.steps %}
{{ funnel_name }}_{{ step_name }} AS (
  SELECT
    {% if steps[step_name].join_key %}
    {{ steps[step_name].join_key }} AS join_key,
    {% endif %}
    {{ data_sources[steps[step_name].data_source].submission_date_column }} AS submission_date,
    {{ steps[step_name].select_expression }} AS column
  FROM 
    {{ data_sources[steps[step_name].data_source].from_expression }}
  {% if not loop.first and steps[step_name].depends_on_previous_step %}
  INNER JOIN {{ funnel_name }}_{{ loop.previtem[0] }} AS prev
  ON 
    prev.submission_date = submission_date AND
    prev.join_key = join_key
  {% endif %}
  WHERE
    {{ data_sources[steps[step_name].data_source].submission_date_column }} = @submission_date
    {% if steps[step_name].where_expression %}
    AND {{ steps[step_name].where_expression }}
    {% endif %}
),
{% endfor %}
{% endfor %}

{% for funnel_name, funnel in funnels.items() %}
{% for step_name in funnel.steps %}
{{ funnel_name }}_{{ step_name }}_aggregated AS (
  SELECT
    submission_date,
    "{{ funnel_name }}" AS funnel,
    {{ steps[step_name].aggregation.sql("column") }} AS aggregated
  FROM
    {{ funnel_name }}_{{ step_name }}
  GROUP BY
    submission_date, 
    funnel
){% if not loop.last %},{% endif %}
{% endfor %}
{% endfor %}

SELECT
  submission_date,
  funnel,
  {% for funnel_name, funnel in funnels.items() %}
    {% for step_name in funnel.steps %}
    {{ funnel_name }}_{{ step_name }}_aggregated.aggregated AS {{ step_name }},
    {% endfor %}
  {% endfor %}
FROM
{% for funnel_name, funnel in funnels.items() %}
  {% for step_name in funnel.steps %}
    {% if loop.first %}
    {{ funnel_name }}_{{ step_name }}_aggregated
    {% else %}
    FULL OUTER JOIN {{ funnel_name }}_{{ step_name }}_aggregated
    USING (submission_date, funnel) 
    {% endif %}
  {% endfor %}
{% endfor %}
