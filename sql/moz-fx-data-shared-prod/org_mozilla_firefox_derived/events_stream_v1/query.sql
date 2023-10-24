-- Query for org_mozilla_firefox_derived.events_stream_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  * EXCEPT (metrics, events, name, category, extra, timestamp) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_firefox",
      client_info.app_build
    ).channel AS normalized_channel,
    STRUCT(
      client_info.app_build AS app_build,
      client_info.app_channel AS app_channel,
      client_info.app_display_version AS app_display_version,
      client_info.architecture AS architecture,
      client_info.device_manufacturer AS device_manufacturer,
      client_info.device_model AS device_model,
      client_info.first_run_date AS first_run_date,
      client_info.locale AS locale,
      client_info.os AS os,
      client_info.os_version AS os_version,
      client_info.telemetry_sdk_build AS telemetry_sdk_build,
      client_info.build_date AS build_date
    ) AS client_info,
    STRUCT(
      ping_info.end_time,
      ping_info.ping_type,
      ping_info.seq,
      ping_info.start_time,
      ping_info.parsed_start_time,
      ping_info.parsed_end_time
    ) AS ping_info
  ),
  client_info.client_id AS client_id,
  ping_info.reason AS reason,
  `mozfun.json.from_map`(ping_info.experiments) AS experiments,
  SAFE.TIMESTAMP_ADD(
    ping_info.parsed_start_time,
    INTERVAL event.timestamp MILLISECOND
  ) AS event_timestamp,
  event.category AS event_category,
  event.name AS event_name,
  `mozfun.json.from_map`(event.extra) AS event_extra,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.events`
CROSS JOIN
  UNNEST(events) AS event
WHERE
  DATE(submission_timestamp) = @submission_date
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  * EXCEPT (metrics, events, name, category, extra, timestamp) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_firefox",
      client_info.app_build
    ).channel AS normalized_channel,
    STRUCT(
      client_info.app_build AS app_build,
      client_info.app_channel AS app_channel,
      client_info.app_display_version AS app_display_version,
      client_info.architecture AS architecture,
      client_info.device_manufacturer AS device_manufacturer,
      client_info.device_model AS device_model,
      client_info.first_run_date AS first_run_date,
      client_info.locale AS locale,
      client_info.os AS os,
      client_info.os_version AS os_version,
      client_info.telemetry_sdk_build AS telemetry_sdk_build,
      client_info.build_date AS build_date
    ) AS client_info,
    STRUCT(
      ping_info.end_time,
      ping_info.ping_type,
      ping_info.seq,
      ping_info.start_time,
      ping_info.parsed_start_time,
      ping_info.parsed_end_time
    ) AS ping_info
  ),
  client_info.client_id AS client_id,
  ping_info.reason AS reason,
  `mozfun.json.from_map`(ping_info.experiments) AS experiments,
  SAFE.TIMESTAMP_ADD(
    ping_info.parsed_start_time,
    INTERVAL event.timestamp MILLISECOND
  ) AS event_timestamp,
  event.category AS event_category,
  event.name AS event_name,
  `mozfun.json.from_map`(event.extra) AS event_extra,
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.events`
CROSS JOIN
  UNNEST(events) AS event
WHERE
  DATE(submission_timestamp) = @submission_date
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  * EXCEPT (metrics, events, name, category, extra, timestamp) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_fenix",
      client_info.app_build
    ).channel AS normalized_channel,
    STRUCT(
      client_info.app_build AS app_build,
      client_info.app_channel AS app_channel,
      client_info.app_display_version AS app_display_version,
      client_info.architecture AS architecture,
      client_info.device_manufacturer AS device_manufacturer,
      client_info.device_model AS device_model,
      client_info.first_run_date AS first_run_date,
      client_info.locale AS locale,
      client_info.os AS os,
      client_info.os_version AS os_version,
      client_info.telemetry_sdk_build AS telemetry_sdk_build,
      client_info.build_date AS build_date
    ) AS client_info,
    STRUCT(
      ping_info.end_time,
      ping_info.ping_type,
      ping_info.seq,
      ping_info.start_time,
      ping_info.parsed_start_time,
      ping_info.parsed_end_time
    ) AS ping_info
  ),
  client_info.client_id AS client_id,
  ping_info.reason AS reason,
  `mozfun.json.from_map`(ping_info.experiments) AS experiments,
  SAFE.TIMESTAMP_ADD(
    ping_info.parsed_start_time,
    INTERVAL event.timestamp MILLISECOND
  ) AS event_timestamp,
  event.category AS event_category,
  event.name AS event_name,
  `mozfun.json.from_map`(event.extra) AS event_extra,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.events`
CROSS JOIN
  UNNEST(events) AS event
WHERE
  DATE(submission_timestamp) = @submission_date
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  * EXCEPT (metrics, events, name, category, extra, timestamp) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_fenix_nightly",
      client_info.app_build
    ).channel AS normalized_channel,
    STRUCT(
      client_info.app_build AS app_build,
      client_info.app_channel AS app_channel,
      client_info.app_display_version AS app_display_version,
      client_info.architecture AS architecture,
      client_info.device_manufacturer AS device_manufacturer,
      client_info.device_model AS device_model,
      client_info.first_run_date AS first_run_date,
      client_info.locale AS locale,
      client_info.os AS os,
      client_info.os_version AS os_version,
      client_info.telemetry_sdk_build AS telemetry_sdk_build,
      client_info.build_date AS build_date
    ) AS client_info,
    STRUCT(
      ping_info.end_time,
      ping_info.ping_type,
      ping_info.seq,
      ping_info.start_time,
      ping_info.parsed_start_time,
      ping_info.parsed_end_time
    ) AS ping_info
  ),
  client_info.client_id AS client_id,
  ping_info.reason AS reason,
  `mozfun.json.from_map`(ping_info.experiments) AS experiments,
  SAFE.TIMESTAMP_ADD(
    ping_info.parsed_start_time,
    INTERVAL event.timestamp MILLISECOND
  ) AS event_timestamp,
  event.category AS event_category,
  event.name AS event_name,
  `mozfun.json.from_map`(event.extra) AS event_extra,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.events`
CROSS JOIN
  UNNEST(events) AS event
WHERE
  DATE(submission_timestamp) = @submission_date
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  * EXCEPT (metrics, events, name, category, extra, timestamp) REPLACE(
    mozfun.norm.fenix_app_info(
      "org_mozilla_fennec_aurora",
      client_info.app_build
    ).channel AS normalized_channel,
    STRUCT(
      client_info.app_build AS app_build,
      client_info.app_channel AS app_channel,
      client_info.app_display_version AS app_display_version,
      client_info.architecture AS architecture,
      client_info.device_manufacturer AS device_manufacturer,
      client_info.device_model AS device_model,
      client_info.first_run_date AS first_run_date,
      client_info.locale AS locale,
      client_info.os AS os,
      client_info.os_version AS os_version,
      client_info.telemetry_sdk_build AS telemetry_sdk_build,
      client_info.build_date AS build_date
    ) AS client_info,
    STRUCT(
      ping_info.end_time,
      ping_info.ping_type,
      ping_info.seq,
      ping_info.start_time,
      ping_info.parsed_start_time,
      ping_info.parsed_end_time
    ) AS ping_info
  ),
  client_info.client_id AS client_id,
  ping_info.reason AS reason,
  `mozfun.json.from_map`(ping_info.experiments) AS experiments,
  SAFE.TIMESTAMP_ADD(
    ping_info.parsed_start_time,
    INTERVAL event.timestamp MILLISECOND
  ) AS event_timestamp,
  event.category AS event_category,
  event.name AS event_name,
  `mozfun.json.from_map`(event.extra) AS event_extra,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.events`
CROSS JOIN
  UNNEST(events) AS event
WHERE
  DATE(submission_timestamp) = @submission_date
