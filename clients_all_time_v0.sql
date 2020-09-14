CREATE TEMP FUNCTION offsets_to_bytes(offsets ARRAY<INT64>) AS (
  (
    SELECT
      LTRIM(
        STRING_AGG(
          (
            SELECT
              -- CODE_POINTS_TO_BYTES is the best available interface for converting
              -- an array of numeric values to a BYTES field; it requires that values
              -- are valid extended ASCII characters, so we can only aggregate 8 bits
              -- at a time, and then append all those chunks together with STRING_AGG.
              CODE_POINTS_TO_BYTES(
                [
                  BIT_OR(
                    (
                      IF(
                        DIV(n - (8 * i), 8) = 0
                        AND (n - (8 * i)) >= 0,
                        1 << MOD(n - (8 * i), 8),
                        0
                      )
                    )
                  )
                ]
              )
            FROM
              UNNEST(offsets) AS n
          ),
          b''
        ),
        b'\x00'
      )
    FROM
      -- Each iteration handles 8 bits, so 256 iterations gives us
      -- 2048 bits, about 5.6 years worth.
      UNNEST(GENERATE_ARRAY(255, 0, -1)) AS i
  )
);

CREATE OR REPLACE TABLE
  analysis.klukas_clients_all_time_v1
AS
WITH base AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    metadata.geo.country,
  FROM
    telemetry_stable.main_v4
  WHERE
    -- Include all time.
    DATE(submission_timestamp) > '2010-01-01'
    -- Only do one sample_id at a time to limit shuffling.
    AND sample_id = 0
),
-- This per_client ETL is where we can define BYTES fields for capturing history
-- for various usage criteria, and also capture the first-seen and last-seen
-- values for various dimensions.
per_client1 AS (
  SELECT
    client_id,
    sample_id,
    offsets_to_bytes(
      ARRAY_AGG(DISTINCT DATE_DIFF(CURRENT_DATE(), submission_date, DAY))
    ) AS days_seen_bits,
    ARRAY_AGG(country IGNORE NULLS ORDER BY submission_timestamp) AS countries
  FROM
    base
  GROUP BY
    sample_id,
    client_id
),
per_client2 AS (
  SELECT
    client_id,
    sample_id,
    days_seen_bits,
    countries[safe_offset(0)] AS first_seen_country,
    array_reverse(countries)[safe_offset(0)] AS last_seen_country,
  FROM
    per_client1
),
-- This per-country section demonstrates how we can maintain a BYTES field per dimension.
-- For a user that only ever appears in one country, this will recreate the top-level days_seen_bits,
-- but for a client that appears in more than one country, we get an array with one entry per
-- country that client has appeared in, and we have the full history of days on which the client
-- appeared in that country.
per_country AS (
  SELECT
    client_id,
    sample_id,
    country,
    offsets_to_bytes(
      ARRAY_AGG(DISTINCT DATE_DIFF(CURRENT_DATE(), submission_date, DAY))
    ) AS days_seen_bits,
  FROM
    base
  GROUP BY
    sample_id,
    client_id,
    country
),
per_country2 AS (
  SELECT
    client_id,
    sample_id,
    ARRAY_AGG(STRUCT(country AS key, days_seen_bits AS value)) AS days_seen_in_country_bytes
  FROM
    per_country
  GROUP BY
    sample_id,
    client_id
)
SELECT
  *
FROM
  per_client2
LEFT JOIN
  per_country2
USING
  (sample_id, client_id)
