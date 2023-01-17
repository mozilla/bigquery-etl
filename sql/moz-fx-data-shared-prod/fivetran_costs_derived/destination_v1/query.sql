-- Query for fivetran_costs_derived.destination_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  id AS destination_id,
  account_id,
  created_at,
  name AS destination_name,
  region
FROM
  `moz-fx-data-bq-fivetran.fivetran_log.destination`
