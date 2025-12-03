WITH sample AS (
    {% if glean %}
      {% for glean_app_id in glean_app_ids %}
        SELECT
          DATE(submission_timestamp) AS submission_date,
          name AS event,
          category,
          extra,
          sample_id,
          timestamp,
          metadata,
          normalized_channel,
          normalized_os,
          normalized_os_version,
          client_info.client_id as client_id,
          {% for property in user_properties %}
          {{property.src}} as {{property.dest}},
          {% endfor %}
          (
            SELECT
              ARRAY_AGG(STRUCT(key, value.branch AS value))
            FROM
              UNNEST(ping_info.experiments)
          ) AS experiments,
          COUNT(*) OVER (PARTITION BY DATE(submission_timestamp), client_info.client_id) AS client_event_count
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
      *,
      COUNT(*) OVER (PARTITION BY submission_date, client_id) AS client_event_count
    FROM
      {{ source_table }}
    {% endif %}
), events AS (
  SELECT
    * EXCEPT (client_event_count)
  FROM
    sample
  WHERE
    (
      submission_date = @submission_date
      OR (@submission_date IS NULL AND submission_date >= '{{ start_date }}')
    )
    AND client_id IS NOT NULL
    -- filter out overactive clients: they distort the data and can cause the job to fail: https://bugzilla.mozilla.org/show_bug.cgi?id=1730190
    AND client_event_count < 150000
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
    {{ dataset }}.event_types event_types
    USING (category, event)
),
grouped AS (
  -- Workaround for this query failing with `Cannot query rows larger than 100MB limit` error on some days.
  -- Example is https://bugzilla.mozilla.org/show_bug.cgi?id=1986081 where we didn't have any aggregated rows
  -- larger than 100MB, but query still failed.
  -- It seems like adding this CTE forces a simpler, more stable query plan which succeeds.
  SELECT
    submission_date,
    client_id,
    sample_id
  FROM
    joined
  GROUP BY
    submission_date,
    client_id,
    sample_id
)
SELECT
  j.submission_date,
  j.client_id,
  j.sample_id,
  CONCAT(STRING_AGG(j.index, ',' ORDER BY timestamp ASC), ',') AS events,
  -- client info
  {% for property in user_properties %}
    mozfun.stats.mode_last(ARRAY_AGG(
      {% if glean %}{{ property.dest }}{% else %}{{ property.src }}{% endif %}
    )) AS {{ property.dest }},
  {% endfor %}
  -- metadata
  {% if include_metadata_fields %}
  mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.city)) AS city,
  mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.country)) AS country,
  mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.subdivision1)) AS subdivision1,
  {% endif %}
  -- normalized fields
  {% if include_normalized_fields %}
  mozfun.stats.mode_last(ARRAY_AGG(normalized_channel)) AS channel,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_os)) AS os,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_os_version)) AS os_version,
  {% endif %}
  -- ping info
  mozfun.map.mode_last(ARRAY_CONCAT_AGG(experiments)) AS experiments
FROM
  joined AS j
INNER JOIN
  grouped AS g
  ON j.submission_date = g.submission_date
  AND j.client_id = g.client_id
  AND j.sample_id IS NOT DISTINCT FROM g.sample_id
GROUP BY
  j.submission_date,
  j.client_id,
  j.sample_id
