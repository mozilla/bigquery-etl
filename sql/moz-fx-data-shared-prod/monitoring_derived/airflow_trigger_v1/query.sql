SELECT
  id,
  classpath,
  created_date,
  kwargs,
  triggerer_id
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.trigger`
WHERE
  NOT _fivetran_deleted
