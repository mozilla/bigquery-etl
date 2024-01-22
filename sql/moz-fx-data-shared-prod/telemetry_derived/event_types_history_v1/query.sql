WITH all_events AS (
  SELECT
    *
  FROM
    telemetry_derived.deanonymized_events
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
    telemetry_derived.event_types_history_v1
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
    ROW_NUMBER() OVER (
      PARTITION BY
        category,
        event
      ORDER BY
        MIN(timestamp) ASC,
        event_property.key ASC
    ) + ANY_VALUE(max_event_property_index) AS event_property_index,
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
    UNNEST(CAST([] AS ARRAY<STRING>)) skipped_property
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
    UNNEST(value) AS values
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
        UNNEST(value) AS values
    ) AS existing_event_type_values
    ON current_events.category = existing_event_type_values.category
    AND current_events.event = existing_event_type_values.event
    AND event_property.key = existing_event_type_values.existing_event_property.key
    AND event_property.value = existing_event_type_values.values.key
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
    event_property_value_index <= 5000
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
    UNNEST(value) AS values
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
