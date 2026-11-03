-- Daily size of each ping at every stage of the ingestion pipeline.
--
-- Pub/Sub only reports bytes per topic and subscription, so those bytes are
-- split between pings as stage_bytes * (P + k * N) / SUM(P + k * N), where P
-- is the ping's compressed bytes, N its message count, and k the per-message
-- overhead (attributes and metadata) implied by the day's totals:
-- k = (stage_bytes - SUM(P)) / SUM(N).
WITH decoder_jobs AS (
  -- Each decoder reads one raw topic through one subscription and publishes
  -- to one decoded topic, which is measured on one of its subscriptions.
  SELECT
    *
  FROM
    UNNEST(
      [
        STRUCT(
          'structured-raw-decoder' AS decoder_job,
          'structured-raw' AS raw_topic,
          'structured-raw.decoder' AS raw_subscription,
          'structured-decoded' AS decoded_topic,
          'structured-decoded.republisher' AS decoded_subscription
        ),
        (
          'structured-logging-decoder',
          'structured-logging',
          'structured-logging.decoder',
          'structured-decoded',
          'structured-decoded.republisher'
        ),
        (
          'structured-direct-decoder',
          'structured-direct',
          'structured-direct.decoder',
          'structured-decoded',
          'structured-decoded.republisher'
        ),
        (
          'telemetry-raw-decoder',
          'telemetry-raw',
          'telemetry-raw.decoder',
          'telemetry-decoded',
          'telemetry-decoded.bq-sink-payload'
        )
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
-- proportion to their decompressed bytes. Compressed bytes include the few
-- invalid logging messages (under 100 a day), so they end up on valid pings
-- instead of in a separate row like for the other decoders.
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
resource_bytes AS (
  -- Bytes published to each topic and delivered on each subscription. Only
  -- delivered message bytes are billed for subscriptions, not ack or modack
  -- bytes.
  SELECT
    IF(metric = 'pubsub_topic_byte_cost', 'topic', 'subscription') AS resource_type,
    COALESCE(
      mozfun.map.get_key(labels, 'topic_id'),
      mozfun.map.get_key(labels, 'subscription_id')
    ) AS resource,
    SUM(value) AS bytes
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.gcp_monitoring_metrics_hourly_v1`
  WHERE
    submission_date = @submission_date
    AND project_id IN (
      'moz-fx-data-shared-prod',
      'moz-fx-data-beam-prod-11f7',
      'moz-fx-ingestion-sink-prod'
    )
    AND (
      (
        metric = 'pubsub_topic_byte_cost'
        AND mozfun.map.get_key(labels, 'operation_type') = 'publish'
      )
      OR (
        metric = 'pubsub_subscription_byte_cost'
        AND mozfun.map.get_key(labels, 'operation_type') = 'streaming_pull_messages'
      )
    )
  GROUP BY
    ALL
),
-- One row per ping and Pub/Sub stage, so every stage is split the same way.
stages AS (
  SELECT
    pings_imputed.*,
    stage.name AS stage,
    stage.resource_type,
    stage.resource,
    stage.payload_bytes
  FROM
    pings_imputed
  INNER JOIN
    decoder_jobs
    USING (decoder_job)
  CROSS JOIN
    UNNEST(
      [
        STRUCT(
          'raw_topic' AS name,
          'topic' AS resource_type,
          decoder_jobs.raw_topic AS resource,
          pings_imputed.compressed_bytes AS payload_bytes
        ),
        (
          'raw_subscription',
          'subscription',
          decoder_jobs.raw_subscription,
          pings_imputed.compressed_bytes
        ),
        -- Messages that fail validation never reach the decoded topic, so the
        -- rolled up invalid row gets no decoded bytes. Valid pings still
        -- include their own invalid messages, which are under 0.1% of
        -- compressed bytes.
        (
          'decoded_topic',
          'topic',
          decoder_jobs.decoded_topic,
          IF(pings_imputed.message_count > 0, pings_imputed.compressed_bytes, 0)
        ),
        (
          'decoded_subscription',
          'subscription',
          decoder_jobs.decoded_subscription,
          IF(pings_imputed.message_count > 0, pings_imputed.compressed_bytes, 0)
        )
      ]
    ) AS stage
),
with_overhead AS (
  SELECT
    stages.*,
    resource_bytes.bytes AS stage_bytes,
    -- Invalid messages also carry overhead on the raw stages, but they have
    -- no message count, so their overhead (about 4 million messages and 0.01%
    -- of raw bytes a day) is spread over valid messages.
    GREATEST(
      (resource_bytes.bytes - SUM(stages.payload_bytes) OVER stage_window) / NULLIF(
        SUM(stages.message_count) OVER stage_window,
        0
      ),
      0
    ) AS overhead_bytes_per_message
  FROM
    stages
  LEFT JOIN
    resource_bytes
    USING (resource_type, resource)
  WINDOW
    stage_window AS (
      PARTITION BY
        stages.stage,
        stages.resource
    )
),
allocated AS (
  SELECT
    *,
    stage_bytes * SAFE_DIVIDE(
      payload_bytes + overhead_bytes_per_message * message_count,
      SUM(payload_bytes + overhead_bytes_per_message * message_count) OVER (
        PARTITION BY
          stage,
          resource
      )
    ) AS bytes
  FROM
    with_overhead
),
per_ping AS (
  SELECT
    decoder_job,
    document_namespace,
    document_type,
    ANY_VALUE(message_count) AS message_count,
    ANY_VALUE(compressed_bytes) AS compressed_bytes,
    ANY_VALUE(compressed_bytes_imputed) AS compressed_bytes_imputed,
    ANY_VALUE(decompressed_bytes) AS decompressed_bytes,
    ANY_VALUE(IF(stage = 'raw_topic', resource, NULL)) AS raw_topic,
    ANY_VALUE(IF(stage = 'raw_topic', BYTES, NULL)) AS raw_topic_bytes,
    ANY_VALUE(
      IF(stage = 'raw_topic', overhead_bytes_per_message, NULL)
    ) AS raw_topic_overhead_bytes_per_message,
    ANY_VALUE(IF(stage = 'raw_subscription', resource, NULL)) AS raw_subscription,
    ANY_VALUE(IF(stage = 'raw_subscription', BYTES, NULL)) AS raw_pubsub_bytes,
    ANY_VALUE(
      IF(stage = 'raw_subscription', overhead_bytes_per_message, NULL)
    ) AS raw_overhead_bytes_per_message,
    ANY_VALUE(IF(stage = 'decoded_topic', resource, NULL)) AS decoded_topic,
    ANY_VALUE(IF(stage = 'decoded_topic', BYTES, NULL)) AS decoded_topic_bytes,
    ANY_VALUE(
      IF(stage = 'decoded_topic', overhead_bytes_per_message, NULL)
    ) AS decoded_topic_overhead_bytes_per_message,
    ANY_VALUE(IF(stage = 'decoded_subscription', resource, NULL)) AS decoded_subscription,
    ANY_VALUE(IF(stage = 'decoded_subscription', BYTES, NULL)) AS decoded_pubsub_bytes,
    ANY_VALUE(
      IF(stage = 'decoded_subscription', overhead_bytes_per_message, NULL)
    ) AS decoded_overhead_bytes_per_message
  FROM
    allocated
  GROUP BY
    decoder_job,
    document_namespace,
    document_type
),
stable_table_sizes AS (
  SELECT
    REGEXP_REPLACE(dataset_id, r'_stable$', '') AS document_namespace,
    REGEXP_REPLACE(table_id, r'_v[0-9]+$', '') AS table_type,
    byte_size,
    row_count
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.stable_and_derived_table_sizes_v1`
  WHERE
    submission_date = @submission_date
    AND dataset_id LIKE '%\\_stable'
),
stable_tables AS (
  SELECT
    document_namespace,
    -- Telemetry main and first_shutdown pings also write their use counters
    -- to separate *_use_counter tables. Count those bytes with the ping, but
    -- not their rows, which duplicate the ping's own rows.
    IF(
      document_namespace = 'telemetry',
      REGEXP_REPLACE(table_type, r'_use_counter$', ''),
      table_type
    ) AS document_type,
    SUM(byte_size) AS stable_logical_bytes,
    SUM(
      IF(document_namespace = 'telemetry' AND ENDS_WITH(table_type, '_use_counter'), 0, row_count)
    ) AS stable_row_count
  FROM
    stable_table_sizes
  GROUP BY
    ALL
),
with_stable AS (
  SELECT
    per_ping.*,
    -- A few doc types arrive through more than one decoder. Split their stable
    -- table size between decoders by message count so it is not double counted.
    stable_tables.stable_logical_bytes * SAFE_DIVIDE(
      per_ping.message_count,
      SUM(per_ping.message_count) OVER ping_window
    ) AS stable_logical_bytes,
    stable_tables.stable_row_count * SAFE_DIVIDE(
      per_ping.message_count,
      SUM(per_ping.message_count) OVER ping_window
    ) AS stable_row_count
  FROM
    per_ping
  LEFT JOIN
    stable_tables
    USING (document_namespace, document_type)
  WINDOW
    ping_window AS (
      PARTITION BY
        per_ping.document_namespace,
        per_ping.document_type
    )
)
-- Fractions of a byte or message aren't meaningful, so round to integers.
SELECT
  @submission_date AS submission_date,
  decoder_job,
  document_namespace,
  document_type,
  CAST(ROUND(message_count) AS INT64) AS message_count,
  CAST(ROUND(compressed_bytes) AS INT64) AS compressed_bytes,
  compressed_bytes_imputed,
  CAST(ROUND(decompressed_bytes) AS INT64) AS decompressed_bytes,
  raw_topic,
  CAST(ROUND(raw_topic_bytes) AS INT64) AS raw_topic_bytes,
  CAST(
    ROUND(raw_topic_overhead_bytes_per_message) AS INT64
  ) AS raw_topic_overhead_bytes_per_message,
  raw_subscription,
  CAST(ROUND(raw_pubsub_bytes) AS INT64) AS raw_pubsub_bytes,
  CAST(ROUND(raw_overhead_bytes_per_message) AS INT64) AS raw_overhead_bytes_per_message,
  decoded_topic,
  CAST(ROUND(decoded_topic_bytes) AS INT64) AS decoded_topic_bytes,
  CAST(
    ROUND(decoded_topic_overhead_bytes_per_message) AS INT64
  ) AS decoded_topic_overhead_bytes_per_message,
  decoded_subscription,
  CAST(ROUND(decoded_pubsub_bytes) AS INT64) AS decoded_pubsub_bytes,
  CAST(ROUND(decoded_overhead_bytes_per_message) AS INT64) AS decoded_overhead_bytes_per_message,
  CAST(ROUND(stable_logical_bytes) AS INT64) AS stable_logical_bytes,
  CAST(ROUND(stable_row_count) AS INT64) AS stable_row_count
FROM
  with_stable
