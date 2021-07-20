-- Generated via bigquery_etl.feature_usage

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
        {% for filter in filters %}
            {% if loop.first %}
                {{ filter.sql }}
            {% else %}
                AND {{ filter.sql }}
            {% endif %}
        {% endfor %}
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
        {% endfor %}
    {% endfor %}
)
SELECT
    {% for source in sources %}
        {% for measure in source.measures %}
            {% if measure.min_version %}
                IF ('{{ measure.min_version }}' < app_version, COALESCE({{ measure.name }}, CAST(0 AS {{ measure.type }})), NULL)
            {% else %}
                {{ measure.name }}
            {% endif %}
        {% endfor %}
    {% endfor %}
FROM
    all_features
