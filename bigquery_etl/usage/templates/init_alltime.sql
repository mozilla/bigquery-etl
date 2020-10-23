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

CREATE TABLE
   analysis.klukas_{{ name }}_alltime_v3
AS
WITH daily AS (
SELECT
  {{ submission_date }} AS submission_date,
  sample_id,
  client_id,
  {% for measure in measures %}
  {{ measure.sql }} AS {{ measure.name }},
  {% endfor %}
FROM
  {{ ref }}
WHERE
  sample_id = 0
  AND {{ submission_date }} BETWEEN '{{ origin_date }}' AND @submission_date
GROUP BY
  submission_date,
  sample_id,
  client_id )
SELECT
  @submission_date AS as_of_date,
  sample_id,
  client_id,
  {% for measure in measures %}{% for usage in measure.usages %}
  offsets_to_bytes(ARRAY_AGG(IF({{ measure.name }}{{ usage.sql }}, DATE_DIFF(@submission_date, submission_date, DAY), NULL) IGNORE NULLS))
  AS days_{{ usage.name }}_bits,
  {% endfor %}{% endfor %}
FROM
  daily
GROUP BY
  sample_id,
  client_id
