-- Generated via ./bqetl generate glean_usage
WITH firefox_desktop_data_leak_blocker_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "data_leak_blocker" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.data_leak_blocker_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_newtab_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "newtab" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_nimbus_targeting_context_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "nimbus_targeting_context" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.nimbus_targeting_context_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_post_profile_restore_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "post_profile_restore" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.post_profile_restore_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_profile_restore_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "profile_restore" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.profile_restore_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_profiles_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "profiles" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.profiles_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_prototype_no_code_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "prototype_no_code_events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.prototype_no_code_events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_sync_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "sync" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.sync_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_urlbar_keyword_exposure_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "urlbar_keyword_exposure" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.urlbar_keyword_exposure_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_urlbar_potential_exposure_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop" AS app_id,
    "firefox_desktop" AS app_name,
    "Firefox for Desktop" AS normalized_app_name,
    "urlbar_potential_exposure" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.urlbar_potential_exposure_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_crashreporter_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_crashreporter" AS app_id,
    "firefox_crashreporter" AS app_name,
    "Firefox Crash Reporter" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_crashreporter_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_background_defaultagent_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop_background_defaultagent" AS app_id,
    "firefox_desktop_background_defaultagent" AS app_name,
    "Firefox Desktop Default Agent Task" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox" AS app_id,
    "fenix" AS app_name,
    "Firefox for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_home_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox" AS app_id,
    "fenix" AS app_name,
    "Firefox for Android" AS normalized_app_name,
    "home" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_onboarding_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox" AS app_id,
    "fenix" AS app_name,
    "Firefox for Android" AS normalized_app_name,
    "onboarding" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.onboarding_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_beta_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox_beta" AS app_id,
    "fenix" AS app_name,
    "Firefox for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_beta_home_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox_beta" AS app_id,
    "fenix" AS app_name,
    "Firefox for Android" AS normalized_app_name,
    "home" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_beta_onboarding_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox_beta" AS app_id,
    "fenix" AS app_name,
    "Firefox for Android" AS normalized_app_name,
    "onboarding" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.onboarding_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_fenix_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_fenix" AS app_id,
    "fenix" AS app_name,
    "Firefox for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_fenix_home_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_fenix" AS app_id,
    "fenix" AS app_name,
    "Firefox for Android" AS normalized_app_name,
    "home" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_fenix_onboarding_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_fenix" AS app_id,
    "fenix" AS app_name,
    "Firefox for Android" AS normalized_app_name,
    "onboarding" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.onboarding_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefox_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefox" AS app_id,
    "firefox_ios" AS app_name,
    "Firefox for iOS" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefox_first_session_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefox" AS app_id,
    "firefox_ios" AS app_name,
    "Firefox for iOS" AS normalized_app_name,
    "first_session" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.first_session_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefox_onboarding_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefox" AS app_id,
    "firefox_ios" AS app_name,
    "Firefox for iOS" AS normalized_app_name,
    "onboarding" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.onboarding_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxbeta_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxbeta" AS app_id,
    "firefox_ios" AS app_name,
    "Firefox for iOS" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxbeta_first_session_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxbeta" AS app_id,
    "firefox_ios" AS app_name,
    "Firefox for iOS" AS normalized_app_name,
    "first_session" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.first_session_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxbeta_onboarding_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxbeta" AS app_id,
    "firefox_ios" AS app_name,
    "Firefox for iOS" AS normalized_app_name,
    "onboarding" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.onboarding_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_fennec_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_fennec" AS app_id,
    "firefox_ios" AS app_name,
    "Firefox for iOS" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_fennec_first_session_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_fennec" AS app_id,
    "firefox_ios" AS app_name,
    "Firefox for iOS" AS normalized_app_name,
    "first_session" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.first_session_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_fennec_onboarding_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_fennec" AS app_id,
    "firefox_ios" AS app_name,
    "Firefox for iOS" AS normalized_app_name,
    "onboarding" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.onboarding_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_reference_browser_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_reference_browser" AS app_id,
    "reference_browser" AS app_name,
    "Reference Browser" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_reference_browser_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_mozregression_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_mozregression" AS app_id,
    "mozregression" AS app_name,
    "mozregression" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mozphab_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mozphab" AS app_id,
    "mozphab" AS app_name,
    "mozphab" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozphab_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mozilla_mach_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mozilla_mach" AS app_id,
    "mach" AS app_name,
    "mach" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozilla_mach_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_focus_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_focus" AS app_id,
    "focus_ios" AS app_name,
    "Firefox Focus for iOS" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_klar_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_klar" AS app_id,
    "klar_ios" AS app_name,
    "Firefox Klar for iOS" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_focus_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_focus" AS app_id,
    "focus_android" AS app_name,
    "Firefox Focus for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_focus_beta_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_focus_beta" AS app_id,
    "focus_android" AS app_name,
    "Firefox Focus for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_focus_nightly_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_focus_nightly" AS app_id,
    "focus_android" AS app_name,
    "Firefox Focus for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_klar_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_klar" AS app_id,
    "klar_android" AS app_name,
    "Firefox Klar for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mozillavpn_daemonsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mozillavpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "daemonsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mozillavpn_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mozillavpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mozillavpn_extensionsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mozillavpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "extensionsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mozillavpn_main_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mozillavpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "main" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mozillavpn_vpnsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mozillavpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "vpnsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_vpn_daemonsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox_vpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "daemonsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_vpn_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox_vpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_vpn_extensionsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox_vpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "extensionsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_vpn_main_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox_vpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "main" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_firefox_vpn_vpnsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_firefox_vpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "vpnsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_daemonsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "daemonsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_extensionsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "extensionsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_main_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "main" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_vpnsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "vpnsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_network_extension_daemonsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn_network_extension" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "daemonsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_network_extension_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn_network_extension" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_network_extension_extensionsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn_network_extension" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "extensionsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_network_extension_main_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn_network_extension" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "main" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
org_mozilla_ios_firefoxvpn_network_extension_vpnsession_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "org_mozilla_ios_firefoxvpn_network_extension" AS app_id,
    "mozilla_vpn" AS app_name,
    "Mozilla VPN" AS normalized_app_name,
    "vpnsession" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mozillavpn_backend_cirrus_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mozillavpn_backend_cirrus" AS app_id,
    "mozillavpn_backend_cirrus" AS app_name,
    "Mozilla VPN Cirrus Sidecar" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_backend_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
glean_dictionary_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "glean_dictionary" AS app_id,
    "glean_dictionary" AS app_name,
    "Glean Dictionary" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.glean_dictionary_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mdn_fred_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mdn_fred" AS app_id,
    "mdn_fred" AS app_name,
    "MDN" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mdn_fred_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mdn_yari_action_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mdn_yari" AS app_id,
    "mdn_yari" AS app_name,
    "MDN (2022–2025)" AS normalized_app_name,
    "action" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mdn_yari_stable.action_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
mdn_yari_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "mdn_yari" AS app_id,
    "mdn_yari" AS app_name,
    "MDN (2022–2025)" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.mdn_yari_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
bedrock_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "bedrock" AS app_id,
    "bedrock" AS app_name,
    "www.mozilla.org" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
bedrock_interaction_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "bedrock" AS app_id,
    "bedrock" AS app_name,
    "www.mozilla.org" AS normalized_app_name,
    "interaction" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.interaction_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
bedrock_non_interaction_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "bedrock" AS app_id,
    "bedrock" AS app_name,
    "www.mozilla.org" AS normalized_app_name,
    "non_interaction" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.non_interaction_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_background_tasks_background_tasks_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop_background_tasks" AS app_id,
    "firefox_desktop_background_tasks" AS app_name,
    "Firefox Desktop background tasks" AS normalized_app_name,
    "background_tasks" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.background_tasks_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
firefox_desktop_background_tasks_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "firefox_desktop_background_tasks" AS app_id,
    "firefox_desktop_background_tasks" AS app_name,
    "Firefox Desktop background tasks" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
accounts_frontend_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "accounts_frontend" AS app_id,
    "accounts_frontend" AS app_name,
    "Mozilla Accounts Frontend" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.accounts_frontend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
accounts_backend_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "accounts_backend" AS app_id,
    "accounts_backend" AS app_name,
    "Mozilla Accounts Backend" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.accounts_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
accounts_cirrus_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "accounts_cirrus" AS app_id,
    "accounts_cirrus" AS app_name,
    "Mozilla Accounts (Cirrus)" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.accounts_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
monitor_cirrus_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "monitor_cirrus" AS app_id,
    "monitor_cirrus" AS app_name,
    "Mozilla Monitor (Cirrus)" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.monitor_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
debug_ping_view_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "debug_ping_view" AS app_id,
    "debug_ping_view" AS app_name,
    "Glean Debug Ping Viewer" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.debug_ping_view_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
monitor_frontend_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "monitor_frontend" AS app_id,
    "monitor_frontend" AS app_name,
    "Mozilla Monitor (Frontend)" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.monitor_frontend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
monitor_backend_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "monitor_backend" AS app_id,
    "monitor_backend" AS app_name,
    "Mozilla Monitor (Backend)" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.monitor_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
relay_backend_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "relay_backend" AS app_id,
    "relay_backend" AS app_name,
    "Firefox Relay Backend" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.relay_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
gleanjs_docs_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "gleanjs_docs" AS app_id,
    "gleanjs_docs" AS app_name,
    "Glean.js Documentation" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.gleanjs_docs_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
thunderbird_desktop_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "thunderbird_desktop" AS app_id,
    "thunderbird_desktop" AS app_name,
    "Thunderbird" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.thunderbird_desktop_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
thunderbird_crashreporter_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "thunderbird_crashreporter" AS app_id,
    "thunderbird_crashreporter" AS app_name,
    "Thunderbird Crash Reporter" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.thunderbird_crashreporter_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
net_thunderbird_android_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "net_thunderbird_android" AS app_id,
    "thunderbird_android" AS app_name,
    "Thunderbird for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
net_thunderbird_android_beta_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "net_thunderbird_android_beta" AS app_id,
    "thunderbird_android" AS app_name,
    "Thunderbird for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_beta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
net_thunderbird_android_daily_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "net_thunderbird_android_daily" AS app_id,
    "thunderbird_android" AS app_name,
    "Thunderbird for Android" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_daily_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
syncstorage_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "syncstorage" AS app_id,
    "syncstorage" AS app_name,
    "Sync Storage" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.syncstorage_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
glam_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "glam" AS app_id,
    "glam" AS app_name,
    "GLAM" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.glam_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
subscription_platform_backend_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "subscription_platform_backend" AS app_id,
    "subscription_platform_backend" AS app_name,
    "Subscription Platform" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.subscription_platform_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
subscription_platform_frontend_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "subscription_platform_frontend" AS app_id,
    "subscription_platform_frontend" AS app_name,
    "Subscription Platform Frontend" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.subscription_platform_frontend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
experimenter_cirrus_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "experimenter_cirrus" AS app_id,
    "experimenter_cirrus" AS app_name,
    "Experimenter (Cirrus)" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.experimenter_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
experimenter_backend_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "experimenter_backend" AS app_id,
    "experimenter_backend" AS app_name,
    "Experimenter" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.experimenter_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
),
subscription_platform_backend_cirrus_events_v1 AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    "subscription_platform_backend_cirrus" AS app_id,
    "subscription_platform_backend_cirrus" AS app_name,
    "Subscription Platform (Cirrus)" AS normalized_app_name,
    "events" AS ping_type,
    event.category AS event_category,
    event.name AS event_name,
    normalized_channel,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    SUM(LENGTH(TO_JSON_STRING(event.extra))) * 10 AS event_extras_length,
    COUNT(*) * 10 AS total_events,
  FROM
    `moz-fx-data-shared-prod.subscription_platform_backend_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND sample_id
    BETWEEN 0
    AND 9
  GROUP BY
    submission_date,
    app_id,
    app_name,
    normalized_app_name,
    ping_type,
    event_category,
    event_name,
    normalized_channel,
    normalized_country_code,
    app_version
)
SELECT
  *
FROM
  firefox_desktop_data_leak_blocker_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_events_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_newtab_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_nimbus_targeting_context_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_post_profile_restore_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_profile_restore_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_profiles_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_prototype_no_code_events_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_sync_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_urlbar_keyword_exposure_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_urlbar_potential_exposure_v1
UNION ALL
SELECT
  *
FROM
  firefox_crashreporter_events_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_background_defaultagent_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_home_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_onboarding_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_beta_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_beta_home_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_beta_onboarding_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_fenix_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_fenix_home_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_fenix_onboarding_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefox_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefox_first_session_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefox_onboarding_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxbeta_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxbeta_first_session_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxbeta_onboarding_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_fennec_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_fennec_first_session_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_fennec_onboarding_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_reference_browser_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_mozregression_events_v1
UNION ALL
SELECT
  *
FROM
  mozphab_events_v1
UNION ALL
SELECT
  *
FROM
  mozilla_mach_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_focus_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_klar_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_focus_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_focus_beta_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_focus_nightly_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_klar_events_v1
UNION ALL
SELECT
  *
FROM
  mozillavpn_daemonsession_v1
UNION ALL
SELECT
  *
FROM
  mozillavpn_events_v1
UNION ALL
SELECT
  *
FROM
  mozillavpn_extensionsession_v1
UNION ALL
SELECT
  *
FROM
  mozillavpn_main_v1
UNION ALL
SELECT
  *
FROM
  mozillavpn_vpnsession_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_vpn_daemonsession_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_vpn_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_vpn_extensionsession_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_vpn_main_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_vpn_vpnsession_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_daemonsession_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_extensionsession_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_main_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_vpnsession_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_network_extension_daemonsession_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_network_extension_events_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_network_extension_extensionsession_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_network_extension_main_v1
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_network_extension_vpnsession_v1
UNION ALL
SELECT
  *
FROM
  mozillavpn_backend_cirrus_events_v1
UNION ALL
SELECT
  *
FROM
  glean_dictionary_events_v1
UNION ALL
SELECT
  *
FROM
  mdn_fred_events_v1
UNION ALL
SELECT
  *
FROM
  mdn_yari_action_v1
UNION ALL
SELECT
  *
FROM
  mdn_yari_events_v1
UNION ALL
SELECT
  *
FROM
  bedrock_events_v1
UNION ALL
SELECT
  *
FROM
  bedrock_interaction_v1
UNION ALL
SELECT
  *
FROM
  bedrock_non_interaction_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_background_tasks_background_tasks_v1
UNION ALL
SELECT
  *
FROM
  firefox_desktop_background_tasks_events_v1
UNION ALL
SELECT
  *
FROM
  accounts_frontend_events_v1
UNION ALL
SELECT
  *
FROM
  accounts_backend_events_v1
UNION ALL
SELECT
  *
FROM
  accounts_cirrus_events_v1
UNION ALL
SELECT
  *
FROM
  monitor_cirrus_events_v1
UNION ALL
SELECT
  *
FROM
  debug_ping_view_events_v1
UNION ALL
SELECT
  *
FROM
  monitor_frontend_events_v1
UNION ALL
SELECT
  *
FROM
  monitor_backend_events_v1
UNION ALL
SELECT
  *
FROM
  relay_backend_events_v1
UNION ALL
SELECT
  *
FROM
  gleanjs_docs_events_v1
UNION ALL
SELECT
  *
FROM
  thunderbird_desktop_events_v1
UNION ALL
SELECT
  *
FROM
  thunderbird_crashreporter_events_v1
UNION ALL
SELECT
  *
FROM
  net_thunderbird_android_events_v1
UNION ALL
SELECT
  *
FROM
  net_thunderbird_android_beta_events_v1
UNION ALL
SELECT
  *
FROM
  net_thunderbird_android_daily_events_v1
UNION ALL
SELECT
  *
FROM
  syncstorage_events_v1
UNION ALL
SELECT
  *
FROM
  glam_events_v1
UNION ALL
SELECT
  *
FROM
  subscription_platform_backend_events_v1
UNION ALL
SELECT
  *
FROM
  subscription_platform_frontend_events_v1
UNION ALL
SELECT
  *
FROM
  experimenter_cirrus_events_v1
UNION ALL
SELECT
  *
FROM
  experimenter_backend_events_v1
UNION ALL
SELECT
  *
FROM
  subscription_platform_backend_cirrus_events_v1
