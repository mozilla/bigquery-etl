WITH crash_frames AS (
  SELECT
    * REPLACE (FORMAT("0x%x", module_offset) AS module_offset)
  FROM
    `moz-fx-data-shared-prod`.telemetry_derived.crash_frames_v1
  WHERE
    DATE(submission_timestamp) = @submission_date
),
crash_symbols AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.telemetry_derived.crash_symbols_v1
  WHERE
    submission_date = @submission_date
  QUALIFY
    -- symbols may be appended over multiple attempts, so only use the most recent symbols
    1 = ROW_NUMBER() OVER (
      PARTITION BY
        debug_file,
        debug_id,
        module_offset
      ORDER BY
        request_timestamp DESC
    )
),
crash AS (
  SELECT
    submission_timestamp,
    document_id,
    IF(STARTS_WITH(normalized_os, "Windows"), "Windows NT", NULL) AS os,
    payload.metadata.async_shutdown_timeout,
    payload.metadata.ipc_channel_error,
    payload.metadata.oom_allocation_size,
    payload.metadata.moz_crash_reason,
    payload.stack_traces.crash_info.type AS reason,
  FROM
    `moz-fx-data-shared-prod`.telemetry_stable.crash_v4
  WHERE
    DATE(submission_timestamp) = @submission_date
),
symbolicated_frames AS (
  SELECT
    crash_frames.submission_timestamp,
    crash_frames.document_id,
    crash_frames.thread_offset,
    ARRAY_AGG(
      STRUCT(
        crash_frames.module_offset,
        crash_symbols.module,
        crash_symbols.function,
        crash_symbols.function_offset,
        crash_symbols.file,
        crash_symbols.line,
        crash_symbols.inlines
      )
      ORDER BY
        crash_frames.frame_offset
    ) AS frames,
  FROM
    crash_frames
  LEFT JOIN
    crash_symbols
  USING
    (debug_file, debug_id, module_offset)
  GROUP BY
    submission_timestamp,
    document_id,
    thread_offset
),
symbolicated_threads AS (
  SELECT
    submission_timestamp,
    document_id,
    ARRAY_AGG(STRUCT(frames) ORDER BY thread_offset) AS threads,
  FROM
    symbolicated_frames
  GROUP BY
    submission_timestamp,
    document_id
)
SELECT
  crash.submission_timestamp,
  crash.document_id,
  crash.os,
  crash.async_shutdown_timeout,
  crash.ipc_channel_error,
  crash.oom_allocation_size,
  crash.moz_crash_reason,
  crash.reason,
  -- only thread 0 and crashing thread are retained, so overwrite crashing thread to point to the
  -- last thread in the list
  NULLIF(ARRAY_LENGTH(symbolicated_threads.threads), 0) - 1 AS crashing_thread,
  symbolicated_threads.threads,
FROM
  crash
LEFT JOIN
  symbolicated_threads
USING
  (submission_timestamp, document_id)
