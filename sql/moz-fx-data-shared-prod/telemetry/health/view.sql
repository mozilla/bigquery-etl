CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.health`
AS
SELECT
  * REPLACE (
    `moz-fx-data-shared-prod.udf.normalize_metadata`(metadata) AS metadata,
    (
      SELECT AS STRUCT
        payload.* REPLACE (
          STRUCT(
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.sendFailure.undefined') AS INT64
            ) AS undefined,
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.sendFailure.timeout') AS INT64
            ) AS timeout,
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.sendFailure.abort') AS INT64
            ) AS abort,
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eUnreachable') AS INT64
            ) AS e_unreachable,
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eTerminated') AS INT64
            ) AS e_terminated,
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eChannelOpen') AS INT64
            ) AS e_channel_open
          ) AS send_failure,
          STRUCT(
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.pingDiscardedForSize.sync') AS INT64
            ) AS sync,
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.pingDiscardedForSize.prio') AS INT64
            ) AS prio,
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.pingDiscardedForSize.main') AS INT64
            ) AS main,
            SAFE_CAST(
              JSON_EXTRACT(additional_properties, '$.payload.pingDiscardedForSize.crash') AS INT64
            ) AS crash,
            SAFE_CAST(
              JSON_EXTRACT(
                additional_properties,
                "$.payload.pingDiscardedForSize['<unknown>']"
              ) AS INT64
            ) AS unknown
          ) AS ping_discarded_for_size
        )
    ) AS payload
  )
FROM
  `moz-fx-data-shared-prod.telemetry_stable.health_v4`
