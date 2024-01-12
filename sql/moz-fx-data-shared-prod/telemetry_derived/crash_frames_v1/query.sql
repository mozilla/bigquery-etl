SELECT
  submission_timestamp,
  document_id,
  thread_offset,
  frame_offset,
  _module.debug_file,
  _module.debug_id,
  IF(
    _frame.ip LIKE '0x%' AND _module.base_addr LIKE '0x%',
    SAFE_CAST(_frame.ip AS INT64) - SAFE_CAST(_module.base_addr AS INT64),
    NULL
  ) AS module_offset,
FROM
  `moz-fx-data-shared-prod`.telemetry_stable.crash_v4
JOIN
  UNNEST(payload.stack_traces.threads) AS _thread
  WITH OFFSET AS thread_offset
  ON thread_offset = 0
  OR thread_offset = payload.stack_traces.crash_info.crashing_thread
JOIN
  UNNEST(_thread.frames) AS _frame
  WITH OFFSET AS frame_offset
  ON frame_offset < 40
LEFT JOIN
  UNNEST(payload.stack_traces.modules) AS _module
  WITH OFFSET AS _module_offset
  ON _frame.module_index = _module_offset
WHERE
  DATE(submission_timestamp) = @submission_date
  AND payload.metadata.ipc_channel_error IS NULL
  AND payload.stack_traces.crash_info.crashing_thread
  BETWEEN 0
  AND ARRAY_LENGTH(payload.stack_traces.threads) - 1
