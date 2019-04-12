CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.smoot_clients_last_seen_1percent_v1` AS
SELECT
  * EXCEPT (generated_time),
  -- To get the integer value of the rightmost set bit, we take a bitwise AND
  -- of the bit pattern and its complement, then we determine the position of
  -- the bit via base-2 logarithm; see https://stackoverflow.com/a/42747608/1260237
  CAST(LOG(days_seen_bits & -days_seen_bits, 2) AS INT64) AS days_since_seen,
  CAST(LOG(days_visited_5_uri_bits & -days_visited_5_uri_bits, 2) AS INT64) AS days_since_visited_5_uri,
  CAST(LOG(days_opened_dev_tools_bits & -days_opened_dev_tools_bits, 2) AS INT64) AS days_since_opened_dev_tools
FROM
  `moz-fx-data-derived-datasets.telemetry.smoot_clients_last_seen_1percent_raw_v1`
