SELECT
  # Exclude the `metrics` column because this backfill query needs to have a schema that's compatible
  # with the current ETL, and after bigquery-etl#6358 this ETL no longer outputs a `metrics` column,
  # but the deployed table still technically has a `metrics` column.
  * EXCEPT (metrics) REPLACE(ping_info.parsed_start_time AS event_timestamp)
FROM
  `moz-fx-data-shared-prod.relay_backend_derived.events_stream_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
