SELECT
  * REPLACE (ping_info.parsed_start_time AS event_timestamp)
FROM
  `moz-fx-data-shared-prod.syncstorage_derived.events_stream_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
