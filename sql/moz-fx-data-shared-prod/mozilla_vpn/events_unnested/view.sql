-- Created by hand (rather than via ./bqetl generate glean_usage)
-- because the VPN doesn't have an events ping (just a "main" one,
-- in which there are events)
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.events_unnested`
AS
SELECT
  e.* EXCEPT (events, metrics) REPLACE(
    "release" AS normalized_channel,
        -- Order of ping_info fields differs between tables; we're verbose here for compatibility
    STRUCT(
      ping_info.end_time,
      ping_info.experiments,
      ping_info.ping_type,
      ping_info.seq,
      ping_info.start_time,
      ping_info.reason,
      ping_info.parsed_start_time,
      ping_info.parsed_end_time
    ) AS ping_info
  ),
  event.timestamp AS event_timestamp,
  event.category AS event_category,
  event.name AS event_name,
  event.extra AS event_extra,
FROM
  `moz-fx-data-shared-prod.mozilla_vpn.main` AS e
CROSS JOIN
  UNNEST(e.events) AS event
