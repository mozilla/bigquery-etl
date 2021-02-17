DECLARE start_date DATE;

DECLARE end_date DATE;

SET start_date = @start_date;

SET end_date = @end_date;

WITH extracted AS (
  SELECT
    *,
    date(submission_timestamp) AS submission_date,
  FROM
    `mozdata.telemetry.mobile_event`
  WHERE
    DATE(submission_timestamp)
    BETWEEN start_date
    AND end_date
    AND normalized_app_name = "Fennec"
    AND normalized_os = "iOS"
),
meta AS (
  SELECT
    * EXCEPT (events)
  FROM
    extracted
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
    meta t
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
