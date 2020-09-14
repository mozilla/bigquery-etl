/*

The underlying "clients_all_time_v0" table has one row per client for all time,
but this view on top of it explodes the dates so that we get one row per client
per day, similar to the clients_last_seen structure we're already familiar with.

The differences between this view and clients_last_seen are:

- The bit patterns are BYTES rather than INT64
- The bit patterns include all history, so we could use them for LTV-type calculations
  that depend on activity 1 year ago
- This view includes first_seen_date, which previously required a separate table
- We get both first_seen and last_seen values for dimensions like country;
  we can use the first_seen values to ensure that clients are not moving from one
  group to another across different dates
- We have a full history of country values seen on different dates

*/

CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.analysis.klukas_clients_all_time`
AS
WITH exploded AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL i day) AS submission_date,
    DATE_SUB(
      CURRENT_DATE(),
      INTERVAL udf.bits_to_days_since_first_seen(days_seen_bits) day
    ) AS first_seen_date,
    sample_id,
    client_id,
    days_seen_bits >> i AS days_seen_bits,
    first_seen_country,
    last_seen_country,
    ARRAY(
      SELECT AS STRUCT
        key,
        value >> i AS value
      FROM
        UNNEST(days_seen_in_country_bytes)
    ) AS days_seen_in_country_bits,
  FROM
    `moz-fx-data-shared-prod.analysis.klukas_clients_all_time_v1`
  -- The cross join parses each input row into one row per day since the client
  -- was first seen, emulating the format of the existing clients_last_seen table.
  CROSS JOIN
    UNNEST(GENERATE_ARRAY(0, 2048)) AS i
  WHERE BIT_COUNT(days_seen_bits) > 0
)
SELECT
  *,
  udf.bits_to_days_since_seen(days_seen_bits) AS days_since_seen,
FROM
  exploded
