WITH extracted AS (
  SELECT
    *,
    date(submission_timestamp) AS submission_date,
  FROM
    `mozdata.telemetry.mobile_event`,
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = "2021-02-01"
),
meta_ranked AS (
  SELECT
    t AS metadata,
    row_number() OVER (
      PARTITION BY
        client_id,
        submission_date
      ORDER BY
        submission_timestamp DESC
    ) AS _n
  FROM
    extracted t
),
meta_recent AS (
  SELECT
    metadata.*
  FROM
    meta_ranked
  WHERE
    _n = 1
),
unnested AS (
  SELECT
    * EXCEPT (events),
    mozdata.udf.deanonymize_event(event).*
  FROM
    extracted,
    UNNEST(events) AS event
),
counts AS (
  SELECT
    client_id,
    submission_date,
    event_category AS category,
    event_method AS method,
    event_object AS object,
    event_string_value AS string_value,
    COUNT(DISTINCT document_id) AS value
  FROM
    unnested
  GROUP BY
    client_id,
    submission_date,
    category,
    method,
    object,
    string_value
)
SELECT
  *
FROM
  counts
JOIN
  meta_recent
USING
  (client_id, submission_date)
