-- Generated via ./bqetl generate feature_usage
-- Don't edit this file directly.
-- To add new metrics, please update sql_generators/feature_usage/templating.yaml
-- and run `./bqetl generate feature_usage`.

WITH
{% for source in sources %}
{{ source.name }} AS (
    SELECT
    {% for measure in source.measures %}
        {{ measure.sql }} AS {{ measure.name }},
    {% endfor %}
    FROM
        {{ source.ref }}
    WHERE
        {% for filter in source.filters %}
            {% if loop.first %}
                {{ filter.sql }}
            {% else %}
                AND {{ filter.sql }}
            {% endif %}
        {% endfor %}
    {% if source.group_by %}
    GROUP BY
        {% for field in source.group_by %}
        {{ field }} {% if not loop.last %},{% endif %}
        {% endfor %}
    {% endif %}
),
{% endfor %}
all_features AS (
    SELECT 
        *
    FROM
    {% for source in sources %}
        {% if loop.first %}
            {{ source.name }}
        {% else %}
            LEFT JOIN
                {{ source.name }}
            USING (client_id, submission_date)
        {% endif %}
    {% endfor %}
)
SELECT
    client_id,
    submission_date,
    {% for source in sources %}
        {% for measure in source.measures %}
            {% if measure.name != "client_id" and measure.name != "submission_date" %}
                {% if measure.min_version %}
                    IF ('{{ measure.min_version }}' < app_version, COALESCE({{ measure.name }}, CAST(0 AS {{ measure.type }})), NULL) AS {{ measure.name }},
                {% else %}
                    {{ measure.name }},
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endfor %}
FROM
    all_features
