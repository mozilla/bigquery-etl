-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.events_unnested`
AS
SELECT
  e.* EXCEPT (events, metrics) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_firefox",
      client_info.app_build
    ).channel AS normalized_channel,
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.events` AS e
CROSS JOIN
  UNNEST(e.events) AS event
UNION ALL
SELECT
  e.* EXCEPT (events, metrics) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_firefox_beta",
      client_info.app_build
    ).channel AS normalized_channel,
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.events` AS e
CROSS JOIN
  UNNEST(e.events) AS event
UNION ALL
SELECT
  e.* EXCEPT (events, metrics) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_fenix",
      client_info.app_build
    ).channel AS normalized_channel,
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
  `moz-fx-data-shared-prod.org_mozilla_fenix.events` AS e
CROSS JOIN
  UNNEST(e.events) AS event
UNION ALL
SELECT
  e.* EXCEPT (events, metrics) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_fenix_nightly",
      client_info.app_build
    ).channel AS normalized_channel,
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.events` AS e
CROSS JOIN
  UNNEST(e.events) AS event
UNION ALL
SELECT
  e.* EXCEPT (events, metrics) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_fennec_aurora",
      client_info.app_build
    ).channel AS normalized_channel,
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.events` AS e
CROSS JOIN
  UNNEST(e.events) AS event
