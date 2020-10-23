CREATE OR REPLACE VIEW
  analysis.klukas_{{ name }}_alltime_v3_view
AS
SELECT
  as_of_date - i AS submission_date,
  sample_id,
  client_id,
{% for measure in measures %}{% for usage in measure.usages %}
  as_of_date - `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(days_{{ usage.name }}_bits) AS first_{{ usage.name }}_date,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_{{ usage.name }}_bits >> i) AS days_since_{{ usage.name }},
  days_{{ usage.name }}_bits >> i AS days_{{ usage.name }}_bits,
  CAST(CONCAT('0x', TO_HEX(RIGHT(days_{{ usage.name }}_bits >> i, 4))) AS INT64) << 36 >> 36 AS days_{{ usage.name }}_bits28,
  {% endfor %}{% endfor %}
FROM
  `moz-fx-data-shared-prod.analysis.klukas_{{ name }}_alltime_v3`
-- The cross join parses each input row into one row per day since the client
-- was first seen, emulating the format of the existing clients_last_seen table.
CROSS JOIN
  UNNEST(GENERATE_ARRAY(0, 2048)) AS i
