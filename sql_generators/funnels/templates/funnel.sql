-- extract the relevant fields for each funnel step and segment if necessary
{% for funnel_name, funnel in funnels.items() %}
  {% if loop.first %}WITH{% endif %}
  {% for step_name in funnel.steps %}
  {{ funnel_name }}_{{ step_name }} AS (
    SELECT
      {% if steps[step_name].join_key %}
        {{ steps[step_name].join_key }} AS join_key,
      {% endif %}
      {% if funnel.dimensions %}
        {% for dimension_name in funnel.dimensions %}
          {% if not loop.first and steps[step_name].depends_on_previous_step %}
            {{ funnel_name }}_{{ loop.previtem }}.{{ dimension_name }} AS {{ dimension_name }},
          {% elif dimensions[dimension_name].data_source == steps[step_name].data_source %}
            {{ dimensions[dimension_name].select_expression }} AS {{ dimension_name }},
          {% else %}
            dimension_source_{{ dimension_name }}.{{ dimension_name }} AS {{ dimension_name }},
          {% endif %}
        {% endfor %}
      {% endif %}
      {{ data_sources[steps[step_name].data_source].submission_date_column }} AS submission_date,
      {{ data_sources[steps[step_name].data_source].client_id_column }} AS client_id,
      {{ steps[step_name].select_expression }} AS column
    FROM 
      {{ data_sources[steps[step_name].data_source].from_expression }}
    {% if not loop.first and steps[step_name].depends_on_previous_step %}
      INNER JOIN {{ funnel_name }}_{{ loop.previtem }} AS prev
      ON 
        prev.submission_date = {{ data_sources[steps[step_name].data_source].submission_date_column }}  AND
        prev.join_key = {{ steps[step_name].join_key }}
    {% endif %}
    {% if funnel.dimensions %}
      {% for dimension_name in funnel.dimensions %}
        {% if dimensions[dimension_name].data_source != steps[step_name].data_source or 
          (not loop.first and steps[step_name].depends_on_previous_step) %}
        LEFT JOIN (
          SELECT
            {{ data_sources[dimensions[dimension_name].data_source].submission_date_column }} AS submission_date,
            {{ data_sources[dimensions[dimension_name].data_source].client_id_column }} AS client_id,
            {{ dimensions[dimension_name].select_expression }} AS {{ dimension_name }}
          FROM
            {{ data_sources[dimensions[dimension_name].data_source].from_expression }}
          WHERE {{ data_sources[dimensions[dimension_name].data_source].submission_date_column }} = @submission_date
        ) AS dimension_source_{{ dimension_name }}
        ON dimension_source_{{ dimension_name }}.client_id = client_id
        {% endif %}
      {% endfor %}
    {% endif %}
    WHERE
      {{ data_sources[steps[step_name].data_source].submission_date_column }} = @submission_date
      {% if steps[step_name].where_expression %}
        AND {{ steps[step_name].where_expression }}
      {% endif %}
  ),
  {% endfor %}
{% endfor %}

-- aggregate each funnel step value
{% for funnel_name, funnel in funnels.items() %}
  {% for step_name in funnel.steps %}
  {{ funnel_name }}_{{ step_name }}_aggregated AS (
    SELECT
      submission_date,
      "{{ funnel_name }}" AS funnel,
      {% if  funnel.dimensions %}
        {% for dimension_name in funnel.dimensions %}
          {{ dimension_name }},
        {% endfor %}
      {% endif %}
      {{ steps[step_name].aggregation.sql("column") }} AS aggregated
    FROM
      {{ funnel_name }}_{{ step_name }}
    GROUP BY
      {% if  funnel.dimensions %}
        {% for dimension_name in funnel.dimensions %}
          {{ dimension_name }},
        {% endfor %}
      {% endif %}
      submission_date, 
      funnel
  ),
  {% endfor %}
{% endfor %}

-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    {% for dimension_name, dimension in dimensions.items() %}
      {% for funnel_name, funnel in funnels.items() %}
        {% if loop.first %}
          COALESCE(
        {% else %},
        {% endif %}

        {% if funnel.dimensions %}
          {% if dimension_name in funnel.dimensions %}
            {{ funnel_name }}_{{ funnel.steps|first }}_aggregated.{{ dimension_name }}
          {% else %}
            NULL
          {% endif %}
        {% else %}
          NULL
        {% endif %}

        {% if loop.last %}
          ) AS {{ dimension_name }},
        {% endif %}
      {% endfor %}
    {% endfor %}
    submission_date,
    funnel,
    {% for step_name, step in steps.items() %}
      {% for funnel_name, funnel in funnels.items() %}
        {% if loop.first %}
          COALESCE(
        {% else %},
        {% endif %}

        {% if step_name in funnel.steps %}
          {{ funnel_name }}_{{ step_name }}_aggregated.aggregated
        {% else %}
        NULL
        {% endif %}

        {% if loop.last %}
          ) AS {{ step_name }},
        {% endif %}
      {% endfor %}
    {% endfor %}
  FROM
  {% for funnel_name, funnel in funnels.items() %}
    {% set outer_loop = loop %}
    {% for step_name in funnel.steps %}
      {% if loop.first and outer_loop.index == 1 %}
        {{ funnel_name }}_{{ step_name }}_aggregated
      {% else %}
        FULL OUTER JOIN {{ funnel_name }}_{{ step_name }}_aggregated
        USING (
          submission_date, 
          {% if steps[step_name].depends_on_previous_step == False %}
            {% for dimension_name in dimensions.keys() %}
              {{ dimension_name }},
            {% endfor %}
          {% endif %}
          funnel
        ) 
      {% endif %}
    {% endfor %}
  {% endfor %}
)

SELECT * FROM merged_funnels