-- Query for fivetran_costs_derived.incremental_mar_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  measured_date,
  date_trunc(measured_date, month) AS measured_month,
  destination_id,
  connector_id AS connector_name,
  table_name,
  CASE
  WHEN
    lower(free_type) = "paid"
  THEN
    coalesce(incremental_rows, 0)
  END
  AS paid_active_rows,
  CASE
  WHEN
    lower(free_type) != "paid"
  THEN
    coalesce(incremental_rows, 0)
  END
  AS free_active_rows,
  incremental_rows AS total_active_rows
FROM
  `moz-fx-data-bq-fivetran.fivetran_log.incremental_mar`
