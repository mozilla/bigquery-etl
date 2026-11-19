WITH newtab_addon_pings AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    sample_id,
    document_id,
    submission_timestamp,
    ping_info.reason AS reason,
    JSON_VALUE(addon, '$.version') AS newtab_addon_version,
    LAX_BOOL(addon.isSystem) AS is_system,
    -- updateDay is days since 1970-01-01; built-in copies report 0.
    -- SAFE. returns NULL for values outside the DATE range instead of failing the run.
    IF(
      LAX_INT64(addon.updateDay) = 0,
      NULL,
      SAFE.DATE_FROM_UNIX_DATE(LAX_INT64(addon.updateDay))
    ) AS install_date,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.addons`,
    UNNEST(JSON_QUERY_ARRAY(metrics.object.addons_active_addons, '$')) AS addon
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND JSON_VALUE(addon, '$.id') = 'newtab@mozilla.org'
    AND JSON_VALUE(addon, '$.version') IS NOT NULL
),
newtab_addon_pings_ordered AS (
  SELECT
    *,
    -- version on the client's last-received ping of the day
    FIRST_VALUE(newtab_addon_version) OVER (
      PARTITION BY
        client_id
      ORDER BY
        submission_timestamp DESC,
        document_id DESC
    ) AS last_reported_version,
    -- is_system and install_date from the first ping reporting this version
    FIRST_VALUE(is_system) OVER client_version AS first_is_system,
    FIRST_VALUE(install_date) OVER client_version AS first_install_date,
  FROM
    newtab_addon_pings
  WINDOW
    client_version AS (
      PARTITION BY
        client_id,
        newtab_addon_version
      ORDER BY
        submission_timestamp,
        document_id
    )
)
SELECT
  submission_date,
  client_id,
  ANY_VALUE(sample_id) AS sample_id,
  newtab_addon_version,
  ANY_VALUE(first_is_system) AS is_system,
  ANY_VALUE(first_install_date) AS install_date,
  MIN(submission_timestamp) AS first_seen_timestamp,
  MAX(submission_timestamp) AS last_seen_timestamp,
  COUNTIF(reason = 'startup') AS startup_ping_count,
  COUNTIF(reason = 'daily') AS daily_ping_count,
  COUNTIF(reason = 'updated') AS updated_ping_count,
  ANY_VALUE(last_reported_version) = newtab_addon_version AS is_last_reported_version,
FROM
  newtab_addon_pings_ordered
GROUP BY
  submission_date,
  client_id,
  newtab_addon_version
