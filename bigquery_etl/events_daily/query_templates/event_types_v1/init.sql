{{ header }}

CREATE OR REPLACE TABLE
  {{ app_id }}_derived.event_types_v1
PARTITION BY
  submission_date
CLUSTER BY
  category,
  event
AS
WITH source AS (
  {% if glean %}
    SELECT
      DATE(submission_timestamp) AS submission_date,
      TIMESTAMP_ADD(ping_info.parsed_start_time, INTERVAL timestamp MILLISECOND) AS timestamp,
      category,
      name AS event,
      extra,
    FROM
      {{ app_id }}.events e
    CROSS JOIN
      UNNEST(e.events) AS event
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
  ON
    skipped_property = event_property.key
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
  USING
    (category, event, event_property)
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
        STRUCT(event_property AS key, VALUES AS value, event_property_index AS index)
      ) IGNORE NULLS
      ORDER BY
        event_property_index ASC
    ) AS event_properties
  FROM
    primary_event_types
  LEFT JOIN
    per_event_property
  USING
    (category, event)
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
