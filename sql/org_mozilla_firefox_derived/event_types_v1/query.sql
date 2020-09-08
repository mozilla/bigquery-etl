WITH
  current_events AS (
  SELECT
    name AS event,
    * EXCEPT (name)
  FROM
    org_mozilla_firefox.events
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
  ),
  new_primary_event_types AS (
  SELECT
    category,
    event,
    MIN(timestamp) AS first_timestamp,
    ROW_NUMBER() OVER (ORDER BY MIN(timestamp) ASC, category ASC, event ASC)
      + (SELECT MAX(numeric_index) FROM org_mozilla_firefox_derived.event_types_v1) AS numeric_index,
    0 AS max_event_property_index
  FROM
    current_events
  LEFT JOIN
    org_mozilla_firefox_derived.event_types_v1
    USING (category, event)
  WHERE
    event_types_v1.event IS NULL
  GROUP BY
    category,
    event),
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
    MAX(event_property.index) AS max_event_property_index
  FROM
    org_mozilla_firefox_derived.event_types_v1,
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
    MIN(timestamp) AS first_timestamp,
    event_property.key AS event_property,
    ROW_NUMBER() OVER (PARTITION BY category, event ORDER BY MIN(timestamp) ASC) + ANY_VALUE(max_event_property_index) AS event_property_index,
    0 AS max_event_property_value_index
  FROM
    current_events,
    UNNEST(extra)AS event_property
  LEFT JOIN
    (SELECT *
     FROM org_mozilla_firefox_derived.event_types_v1, UNNEST(event_properties)) event_types_v1
    USING (category, event, key)
  JOIN
    all_primary_event_types
    USING (event, category)
  WHERE
    event_types_v1.event IS NULL
  GROUP BY
    category,
    event,
    event_property),
  all_event_property_indices AS (
  SELECT
    *
  FROM
    new_event_property_indices
  UNION ALL
  SELECT
    category,
    event,
    first_timestamp,
    event_property.key AS event_property,
    event_property.index AS event_property_index,
    MAX(values.index) AS max_event_property_value_index
  FROM
    org_mozilla_firefox_derived.event_types_v1,
    UNNEST(event_properties) AS event_property,
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
    MIN(timestamp) AS first_timestamp,
    event_property.key AS event_property,
    event_property.value AS event_property_value,
    ROW_NUMBER() OVER (PARTITION BY current_events.category, current_events.event, event_property.key ORDER BY MIN(timestamp) ASC) 
      + ANY_VALUE(max_event_property_value_index) AS event_property_value_index,
  FROM
    current_events,
    UNNEST(extra) AS event_property
  LEFT JOIN (
    SELECT event_types_v1.* EXCEPT (event_properties), existing_event_property, values
    FROM org_mozilla_firefox_derived.event_types_v1,
         UNNEST(event_properties) AS existing_event_property,
         UNNEST(value) AS values) AS event_types_v1
    ON event_property.key = event_types_v1.existing_event_property.key
    AND event_property.value = event_types_v1.values.key
  JOIN
    all_event_property_indices
    ON all_event_property_indices.category = current_events.category
    AND all_event_property_indices.event = current_events.event
    AND all_event_property_indices.event_property = event_property.key
  WHERE
    event_types_v1.event IS NULL
  GROUP BY
    category,
    event,
    event_property,
    event_property_value
  ), all_event_property_value_indices AS (
  SELECT *
  FROM new_event_property_value_indices
  UNION ALL
  SELECT
    category,
    event,
    first_timestamp,
    event_property.key AS event_property,
    values.key AS event_property_value,
    values.index AS event_property_value_index
  FROM
    org_mozilla_firefox_derived.event_types_v1,
    UNNEST(event_properties) AS event_property,
    UNNEST(value) AS values
  ),
  per_event_property AS (
  SELECT
    category,
    event,
    event_property,
    event_property_index,
    ARRAY_AGG(STRUCT(event_property_value AS key, udf.event_code_points_to_string([event_property_value_index]) AS value, event_property_value_index AS index) ORDER BY event_property_value_index ASC) AS values,
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
    ARRAY_AGG(STRUCT(event_property AS key, values AS value, event_property_index AS index) ORDER BY event_property_index ASC) AS event_properties
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

SELECT *
FROM per_event
ORDER BY numeric_index ASC
