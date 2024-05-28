{% raw %}
{% if is_init() %}
{% endraw%}
WITH source AS (
  {% if glean %}
    {% for glean_app_id in glean_app_ids %}
      SELECT
        DATE(submission_timestamp) AS submission_date,
        SAFE.TIMESTAMP_ADD(ping_info.parsed_start_time, INTERVAL timestamp MILLISECOND) AS timestamp,
        category,
        name AS event,
        extra,
      FROM
        {{ glean_app_id }}.{{ events_table_name }} e
      CROSS JOIN
        UNNEST(e.events) AS event
        {% if not loop.last %}
          UNION ALL
        {% endif %}
    {% endfor %}
    {% else %}
    SELECT
      *
    FROM
      {{ source_table }}
    {% endif %}
),
sample AS (
  SELECT
    *
  FROM
    source
  WHERE
    submission_date >= '{{ start_date }}'
),
primary_event_types AS (
  SELECT
    category,
    event,
    MIN(timestamp) AS first_timestamp,
    ROW_NUMBER() OVER (ORDER BY MIN(timestamp) ASC, category ASC, event ASC) AS primary_index,
  FROM
    sample
  GROUP BY
    category,
    event
),
event_property_indices AS (
  SELECT
    category,
    event,
    MIN(timestamp) AS first_timestamp,
    event_property.key AS event_property,
    ROW_NUMBER() OVER (
      PARTITION BY
        category,
        event
      ORDER BY
        MIN(timestamp) ASC,
        event_property.key ASC
    ) AS event_property_index,
  FROM
    sample,
    UNNEST(extra) AS event_property
  LEFT JOIN
    UNNEST(
      CAST([{% for property in skipped_properties %}'{{ property }}'{% if not loop.last %},{% endif %}{% endfor %}] AS ARRAY<STRING>)
    ) skipped_property
    ON skipped_property = event_property.key
  WHERE
    skipped_property IS NULL
  GROUP BY
    category,
    event,
    event_property
),
event_property_value_indices AS (
  SELECT
    category,
    event,
    MIN(timestamp) AS first_timestamp,
    event_property.key AS event_property,
    event_property.value AS event_property_value,
    ROW_NUMBER() OVER (
      PARTITION BY
        category,
        event,
        event_property.key
      ORDER BY
        MIN(timestamp) ASC,
        event_property.value ASC
    ) AS event_property_value_index,
  FROM
    sample,
    UNNEST(extra) AS event_property
  GROUP BY
    category,
    event,
    event_property,
    event_property_value
),
per_event_property AS (
  SELECT
    category,
    event,
    event_property,
    event_property_index,
    ARRAY_AGG(
      STRUCT(
        event_property_value AS key,
        udf.event_code_points_to_string([event_property_value_index]) AS value,
        event_property_value_index AS index
      )
      ORDER BY
        event_property_value_index ASC
    ) AS values,
  FROM
    event_property_value_indices
  INNER JOIN
    event_property_indices
    USING (category, event, event_property)
  WHERE
    event_property_value_index <= {{ max_property_values }}
  GROUP BY
    category,
    event,
    event_property,
    event_property_index
),
per_event AS (
  SELECT
    category,
    event,
    first_timestamp,
    primary_index AS numeric_index,
    udf.event_code_points_to_string([primary_index]) AS index,
    ARRAY_AGG(
      IF(
        event_property IS NULL,
        NULL,
        STRUCT(event_property AS key, `values` AS value, event_property_index AS index)
      ) IGNORE NULLS
      ORDER BY
        event_property_index ASC
    ) AS event_properties
  FROM
    primary_event_types
  LEFT JOIN
    per_event_property
    USING (category, event)
  GROUP BY
    category,
    event,
    first_timestamp,
    primary_index
),
max_date AS (
  SELECT
    MAX(submission_date) AS submission_date
  FROM
    sample
)
SELECT
  *
FROM
  per_event,
  max_date
{% raw %}
{% else %}
{% endraw %}
WITH all_events AS (
  {% if glean %}
    {% for glean_app_id in glean_app_ids %}
      SELECT
        DATE(submission_timestamp) AS submission_date,
        SAFE.TIMESTAMP_ADD(ping_info.parsed_start_time, INTERVAL timestamp MILLISECOND) AS timestamp,
        category,
        name AS event,
        extra,
      FROM
        {{ glean_app_id }}.{{ events_table_name }} e
      CROSS JOIN
        UNNEST(e.events) AS event
      {% if not loop.last %}
        UNION ALL
      {% endif %}
    {% endfor %}
  {% else %}
      SELECT
        *
      FROM
        {{ source_table }}
  {% endif %}
),
current_events AS (
  SELECT
    *
  FROM
    all_events
  WHERE
    submission_date = @submission_date
),
event_types AS (
  SELECT
    *
  FROM
    {{ dataset }}_derived.event_types_history_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
),
new_primary_event_types AS (
  SELECT
    category,
    event,
    MIN(timestamp) AS first_timestamp,
    ROW_NUMBER() OVER (ORDER BY MIN(timestamp) ASC, category ASC, event ASC) + (
      SELECT
        MAX(numeric_index)
      FROM
        event_types
    ) AS numeric_index,
    0 AS max_event_property_index
  FROM
    current_events
  LEFT JOIN
    event_types
    USING (category, event)
  WHERE
    event_types.event IS NULL
  GROUP BY
    category,
    event
),
all_primary_event_types AS (
  SELECT
    *
  FROM
    new_primary_event_types
  UNION ALL
  SELECT
    category,
    event,
    first_timestamp,
    numeric_index,
    MAX(COALESCE(event_property.index, 0)) AS max_event_property_index
  FROM
    event_types
  LEFT JOIN
    UNNEST(event_properties) AS event_property
  GROUP BY
    category,
    event,
    first_timestamp,
    numeric_index
),
new_event_property_indices AS (
  SELECT
    category,
    event,
    event_property.key AS event_property,
    ROW_NUMBER() OVER (PARTITION BY category, event ORDER BY MIN(timestamp) ASC, event_property.key ASC) + ANY_VALUE(
      max_event_property_index
    ) AS event_property_index,
    0 AS max_event_property_value_index
  FROM
    current_events,
    UNNEST(extra) AS event_property
  LEFT JOIN
    (SELECT * FROM event_types, UNNEST(event_properties)) event_types
    USING (category, event, key)
  JOIN
    all_primary_event_types
    USING (event, category)
  LEFT JOIN
    UNNEST(
      CAST([{% for property in skipped_properties %}'{{ property }}'{% if not loop.last %},{% endif %}{% endfor %}] AS ARRAY<STRING>)
    ) skipped_property
    ON skipped_property = event_property.key
  WHERE
    skipped_property IS NULL
    AND event_types.event IS NULL
  GROUP BY
    category,
    event,
    event_property
),
all_event_property_indices AS (
  SELECT
    *
  FROM
    new_event_property_indices
  UNION ALL
  SELECT
    category,
    event,
    event_property.key AS event_property,
    event_property.index AS event_property_index,
    MAX(COALESCE(`values`.index, 0)) AS max_event_property_value_index
  FROM
    event_types
  LEFT JOIN
    UNNEST(event_properties) AS event_property
  LEFT JOIN
    UNNEST(value) AS `values`
  GROUP BY
    category,
    event,
    first_timestamp,
    event_property,
    event_property_index
),
new_event_property_value_indices AS (
  SELECT
    current_events.category,
    current_events.event,
    event_property.key AS event_property,
    event_property.value AS event_property_value,
    ROW_NUMBER() OVER (
      PARTITION BY
        current_events.category,
        current_events.event,
        event_property.key
      ORDER BY
        MIN(timestamp) ASC,
        event_property.value ASC
    ) + ANY_VALUE(max_event_property_value_index) AS event_property_value_index,
  FROM
    current_events,
    UNNEST(extra) AS event_property
  LEFT JOIN
    (
      SELECT
        event_types.* EXCEPT (event_properties),
        existing_event_property,
        `values`
      FROM
        event_types,
        UNNEST(event_properties) AS existing_event_property,
        UNNEST(value) AS `values`
    ) AS existing_event_type_values
    ON current_events.category = existing_event_type_values.category
    AND current_events.event = existing_event_type_values.event
    AND event_property.key = existing_event_type_values.existing_event_property.key
    AND event_property.value = existing_event_type_values.`values`.key
  JOIN
    all_event_property_indices
    ON all_event_property_indices.category = current_events.category
    AND all_event_property_indices.event = current_events.event
    AND all_event_property_indices.event_property = event_property.key
  WHERE
    existing_event_type_values.event IS NULL
  GROUP BY
    category,
    event,
    event_property,
    event_property_value
),
all_event_property_value_indices AS (
  SELECT
    *
  FROM
    new_event_property_value_indices
  WHERE
    -- Doesn't remove historical event_property values
    event_property_value_index <= {{ max_property_values }}
  UNION ALL
  SELECT
    category,
    event,
    event_property.key AS event_property,
    `values`.key AS event_property_value,
    `values`.index AS event_property_value_index
  FROM
    event_types,
    UNNEST(event_properties) AS event_property,
    UNNEST(value) AS `values`
),
per_event_property AS (
  SELECT
    category,
    event,
    event_property,
    event_property_index,
    ARRAY_AGG(
      STRUCT(
        event_property_value AS key,
        udf.event_code_points_to_string([event_property_value_index]) AS value,
        event_property_value_index AS index
      )
      ORDER BY
        event_property_value_index ASC
    ) AS `values`,
  FROM
    all_event_property_value_indices
  INNER JOIN
    all_event_property_indices
    USING (category, event, event_property)
  GROUP BY
    category,
    event,
    event_property,
    event_property_index
),
per_event AS (
  SELECT
    category,
    event,
    first_timestamp,
    numeric_index,
    udf.event_code_points_to_string([numeric_index]) AS index,
    ARRAY_AGG(
      IF(
        event_property IS NULL,
        NULL,
        STRUCT(event_property AS key, `values` AS value, event_property_index AS index)
      ) IGNORE NULLS
      ORDER BY
        event_property_index ASC
    ) AS event_properties
  FROM
    all_primary_event_types
  LEFT JOIN
    per_event_property
    USING (category, event)
  GROUP BY
    category,
    event,
    first_timestamp,
    numeric_index
)
SELECT
  @submission_date AS submission_date,
  *
FROM
  per_event
{% raw %}
{% endif %}
{% endraw %}
