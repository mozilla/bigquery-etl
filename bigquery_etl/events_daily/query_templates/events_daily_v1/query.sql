WITH sample AS (
    {% if glean %}
    SELECT
      DATE(submission_timestamp) AS submission_date,
      name AS event,
      client_info.*,
      (
        SELECT
          ARRAY_AGG(STRUCT(key, value.branch AS value))
        FROM
          UNNEST(ping_info.experiments)
      ) AS experiments,
      * EXCEPT (name, client_info)
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
), events AS (
  SELECT
    *
  FROM
    sample
  WHERE
    submission_date = @submission_date
    OR (@submission_date IS NULL AND submission_date >= '{{ start_date }}')
),
joined AS (
  SELECT
    CONCAT(
      udf.pack_event_properties(events.extra, event_types.event_properties),
      index
    ) AS index,
    events.* EXCEPT (category, event, extra)
  FROM
    events
  INNER JOIN
    {{ app_id }}.event_types event_types
  USING
    (category, event)
)
SELECT
  submission_date,
  client_id,
  sample_id,
  CONCAT(STRING_AGG(index, ',' ORDER BY timestamp ASC), ',') AS events,
  -- client info
  {% for property in user_properties %}
    mozfun.stats.mode_last(ARRAY_AGG({{ property.src }})) AS {{ property.dest }},
  {% endfor %}
  -- metadata
  mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.city)) AS city,
  mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.country)) AS country,
  mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.subdivision1)) AS subdivision1,
  -- normalized fields
  {% if include_normalized_fields %}
  mozfun.stats.mode_last(ARRAY_AGG(normalized_channel)) AS channel,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_os)) AS os,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_os_version)) AS os_version,
  {% endif %}
  -- ping info
  mozfun.map.mode_last(ARRAY_CONCAT_AGG(experiments)) AS experiments
FROM
  joined
GROUP BY
  submission_date,
  client_id,
  sample_id
