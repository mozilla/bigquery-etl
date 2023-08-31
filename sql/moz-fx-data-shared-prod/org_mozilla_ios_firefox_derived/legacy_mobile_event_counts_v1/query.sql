WITH extracted AS (
  SELECT
    *,
    DATE(submission_timestamp) AS submission_date,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.mobile_event_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
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
    ROW_NUMBER() OVER (
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
    `moz-fx-data-shared-prod`.udf.deanonymize_event(event).*
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
    COUNT(*) AS value
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
  USING (client_id, submission_date)
