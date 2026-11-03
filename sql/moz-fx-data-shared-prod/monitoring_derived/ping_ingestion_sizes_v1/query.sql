-- Daily size of each ping at every stage of the ingestion pipeline.
--
-- Pub/Sub only reports bytes per subscription, so subscription bytes are split
-- between pings as stage_bytes * (P + k * N) / SUM(P + k * N), where P is the
-- ping's compressed bytes, N its message count, and k the per-message overhead
-- (attributes and metadata) implied by the day's totals:
-- k = (stage_bytes - SUM(P)) / SUM(N).
WITH decoder_jobs AS (
  -- Each decoder reads one raw subscription and writes to one decoded topic.
  SELECT
    *
  FROM
    UNNEST(
      [
        STRUCT(
          'structured-raw-decoder' AS decoder_job,
          'structured-raw.decoder' AS raw_subscription,
          'structured-decoded.republisher' AS decoded_subscription
        ),
        (
          'structured-logging-decoder',
          'structured-logging.decoder',
          'structured-decoded.republisher'
        ),
        (
          'structured-direct-decoder',
          'structured-direct.decoder',
          'structured-decoded.republisher'
        ),
        ('telemetry-raw-decoder', 'telemetry-raw.decoder', 'telemetry-decoded.bq-sink-payload')
      ]
    )
),
counters AS (
  SELECT
    metric,
    -- Job names end in a hash and a counter that change on redeploys.
    REGEXP_REPLACE(
      mozfun.map.get_key(labels, 'job_name'),
      r'-[0-9a-f]{8}-[0-9]+$',
      ''
    ) AS decoder_job,
    SPLIT(mozfun.map.get_key(labels, 'metric_name'), '/')[SAFE_OFFSET(0)] AS document_namespace,
    -- Telemetry predecompress counters have no version suffix, so drop it
    -- everywhere to make the counters joinable.
    REGEXP_REPLACE(
      SPLIT(mozfun.map.get_key(labels, 'metric_name'), '/')[SAFE_OFFSET(1)],
      r'_v[0-9]+$',
      ''
    ) AS document_type,
    value
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.gcp_monitoring_metrics_hourly_v1`
  WHERE
    submission_date = @submission_date
    AND project_id = 'moz-fx-data-beam-prod-11f7'
    AND metric IN (
      'dataflow_predecompress_submission_bytes',
      'dataflow_valid_submission_bytes',
      'dataflow_valid_submission'
    )
),
pings AS (
  SELECT
    decoder_job,
    document_namespace,
    document_type,
    SUM(IF(metric = 'dataflow_valid_submission', value, 0)) AS message_count,
    SUM(IF(metric = 'dataflow_predecompress_submission_bytes', value, 0)) AS compressed_bytes,
    SUM(IF(metric = 'dataflow_valid_submission_bytes', value, 0)) AS decompressed_bytes
  FROM
    counters
  INNER JOIN
    decoder_jobs
    USING (decoder_job)
  GROUP BY
    ALL
),
-- The logging decoder reports compressed bytes only as
-- unknown_namespace/unknown_doctype. Give its pings compressed bytes in
-- proportion to their decompressed bytes.
logging_compression AS (
  SELECT
    SAFE_DIVIDE(SUM(compressed_bytes), SUM(decompressed_bytes)) AS ratio
  FROM
    pings
  WHERE
    decoder_job = 'structured-logging-decoder'
),
pings_imputed AS (
  SELECT
    decoder_job,
    -- Submissions to namespaces or doc types with no valid messages are mostly
    -- junk, so they are rolled up into one row per decoder.
    IF(message_count > 0, document_namespace, NULL) AS document_namespace,
    IF(message_count > 0, document_type, NULL) AS document_type,
    decoder_job = 'structured-logging-decoder' AS compressed_bytes_imputed,
    SUM(message_count) AS message_count,
    SUM(
      IF(decoder_job = 'structured-logging-decoder', decompressed_bytes * ratio, compressed_bytes)
    ) AS compressed_bytes,
    SUM(decompressed_bytes) AS decompressed_bytes
  FROM
    pings
  CROSS JOIN
    logging_compression
  WHERE
    NOT (decoder_job = 'structured-logging-decoder' AND document_namespace = 'unknown_namespace')
  GROUP BY
    ALL
),
subscription_bytes AS (
  SELECT
    mozfun.map.get_key(labels, 'subscription_id') AS subscription_id,
    SUM(value) AS bytes
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.gcp_monitoring_metrics_hourly_v1`
  WHERE
    submission_date = @submission_date
    AND project_id IN ('moz-fx-data-beam-prod-11f7', 'moz-fx-ingestion-sink-prod')
    AND metric = 'pubsub_subscription_byte_cost'
    -- Only delivered message bytes are billed, not ack or modack bytes.
    AND mozfun.map.get_key(labels, 'operation_type') = 'streaming_pull_messages'
  GROUP BY
    subscription_id
),
with_stage_totals AS (
  SELECT
    pings_imputed.*,
    decoder_jobs.raw_subscription,
    decoder_jobs.decoded_subscription,
    raw.bytes AS raw_subscription_bytes,
    decoded.bytes AS decoded_subscription_bytes
  FROM
    pings_imputed
  INNER JOIN
    decoder_jobs
    USING (decoder_job)
  LEFT JOIN
    subscription_bytes AS raw
    ON raw.subscription_id = decoder_jobs.raw_subscription
  LEFT JOIN
    subscription_bytes AS decoded
    ON decoded.subscription_id = decoder_jobs.decoded_subscription
),
with_overhead AS (
  SELECT
    *,
    GREATEST(
      (
        raw_subscription_bytes - SUM(compressed_bytes) OVER (PARTITION BY raw_subscription)
      ) / NULLIF(SUM(message_count) OVER (PARTITION BY raw_subscription), 0),
      0
    ) AS raw_overhead_bytes_per_message,
    GREATEST(
      (
        decoded_subscription_bytes - SUM(compressed_bytes) OVER (PARTITION BY decoded_subscription)
      ) / NULLIF(SUM(message_count) OVER (PARTITION BY decoded_subscription), 0),
      0
    ) AS decoded_overhead_bytes_per_message
  FROM
    with_stage_totals
),
allocated AS (
  SELECT
    *,
    raw_subscription_bytes * SAFE_DIVIDE(
      compressed_bytes + raw_overhead_bytes_per_message * message_count,
      SUM(compressed_bytes + raw_overhead_bytes_per_message * message_count) OVER (
        PARTITION BY
          raw_subscription
      )
    ) AS raw_pubsub_bytes,
    decoded_subscription_bytes * SAFE_DIVIDE(
      compressed_bytes + decoded_overhead_bytes_per_message * message_count,
      SUM(compressed_bytes + decoded_overhead_bytes_per_message * message_count) OVER (
        PARTITION BY
          decoded_subscription
      )
    ) AS decoded_pubsub_bytes
  FROM
    with_overhead
),
stable_tables AS (
  SELECT
    REGEXP_REPLACE(dataset_id, r'_stable$', '') AS document_namespace,
    REGEXP_REPLACE(table_id, r'_v[0-9]+$', '') AS document_type,
    SUM(byte_size) AS stable_logical_bytes,
    SUM(row_count) AS stable_row_count
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.stable_and_derived_table_sizes_v1`
  WHERE
    submission_date = @submission_date
    AND dataset_id LIKE '%\\_stable'
  GROUP BY
    ALL
)
SELECT
  @submission_date AS submission_date,
  allocated.decoder_job,
  allocated.document_namespace,
  allocated.document_type,
  allocated.message_count,
  allocated.compressed_bytes,
  allocated.compressed_bytes_imputed,
  allocated.decompressed_bytes,
  allocated.raw_subscription,
  allocated.raw_pubsub_bytes,
  allocated.raw_overhead_bytes_per_message,
  allocated.decoded_subscription,
  allocated.decoded_pubsub_bytes,
  allocated.decoded_overhead_bytes_per_message,
  -- A few doc types arrive through more than one decoder. Split their stable
  -- table size between decoders by message count so it is not double counted.
  stable_tables.stable_logical_bytes * SAFE_DIVIDE(
    allocated.message_count,
    SUM(allocated.message_count) OVER (
      PARTITION BY
        allocated.document_namespace,
        allocated.document_type
    )
  ) AS stable_logical_bytes,
  stable_tables.stable_row_count * SAFE_DIVIDE(
    allocated.message_count,
    SUM(allocated.message_count) OVER (
      PARTITION BY
        allocated.document_namespace,
        allocated.document_type
    )
  ) AS stable_row_count
FROM
  allocated
LEFT JOIN
  stable_tables
  USING (document_namespace, document_type)
