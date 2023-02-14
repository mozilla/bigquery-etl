-- Query for telemetry_derived.test_query_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  DATE('2023-01-01') AS submission_date,
  123 AS t,
  "abc" AS e
