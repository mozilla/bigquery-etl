-- Query for org_mozilla_fenix_derived.recorded_targeting_context_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
WITH filtered AS (
  SELECT
    m.client_info.client_id AS client_id,
    m.submission_timestamp,
    m.metrics.object.nimbus_system_recorded_nimbus_context AS context
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.metrics` m
  WHERE
    1 = 1
    AND m.metrics.object.nimbus_system_recorded_nimbus_context IS NOT NULL
    AND DATE(m.submission_timestamp) = @submission_date
    AND m.normalized_channel = 'release'
    AND m.app_version_major >= 135
)
SELECT
  m.client_id,
  DATE(m.submission_timestamp) AS submission_date,
  COALESCE(
    CAST(JSON_VALUE(context, '$.android_sdk_version') AS int),
    CAST(JSON_VALUE(context, '$.androidSdkVersion') AS int)
  ) AS androidSdkVersion,
  COALESCE(
    CAST(JSON_VALUE(context, '$.app_version') AS string),
    CAST(JSON_VALUE(context, '$.appVersion') AS string)
  ) AS appVersion,
  COALESCE(
    CAST(JSON_VALUE(context, '$.days_since_install') AS int),
    CAST(JSON_VALUE(context, '$.daysSinceInstall') AS int)
  ) AS daysSinceInstall,
  COALESCE(
    CAST(JSON_VALUE(context, '$.days_since_update') AS int),
    CAST(JSON_VALUE(context, '$.daysSinceUpdate') AS int)
  ) AS daysSinceUpdate,
  COALESCE(
    CAST(JSON_VALUE(context, '$.device_manufacturer') AS string),
    CAST(JSON_VALUE(context, '$.deviceManufacturer') AS string)
  ) AS deviceManufacturer,
  COALESCE(
    CAST(JSON_VALUE(context, '$.device_model') AS string),
    CAST(JSON_VALUE(context, '$.deviceModel') AS string)
  ) AS deviceModel,
  COALESCE(
    CAST(JSON_VALUE(context, '$.event_query_values.days_opened_in_last_28') AS int),
    CAST(JSON_VALUE(context, '$.eventQueryValues.daysOpenedInLast28') AS int)
  ) AS eventQuery_daysOpenedInLast28,
  COALESCE(
    CAST(JSON_VALUE(context, '$.install_referrer_response_utm_Campaign') AS string),
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmCampaign') AS string)
  ) AS installReferrerResponseUtmCampaign,
  COALESCE(
    CAST(JSON_VALUE(context, '$.install_referrer_response_utm_content') AS string),
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmContent') AS string)
  ) AS installReferrerResponseUtmContent,
  COALESCE(
    CAST(JSON_VALUE(context, '$.install_referrer_response_utm_medium') AS string),
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmMedium') AS string)
  ) AS installReferrerResponseUtmMedium,
  COALESCE(
    CAST(JSON_VALUE(context, '$.install_referrer_response_utm_source') AS string),
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmSource') AS string)
  ) AS installReferrerResponseUtmSource,
  COALESCE(
    CAST(JSON_VALUE(context, '$.install_referrer_response_utm_term') AS string),
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmTerm') AS string)
  ) AS installReferrerResponseUtmTerm,
  COALESCE(
    CAST(JSON_VALUE(context, '$.is_first_run') AS boolean),
    CAST(JSON_VALUE(context, '$.isFirstRun') AS boolean)
  ) AS isFirstRun,
  CAST(JSON_VALUE(context, '$.language') AS string) AS language,
  CAST(JSON_VALUE(context, '$.locale') AS string) AS locale,
  CAST(JSON_VALUE(context, '$.region') AS string) AS region,
  context
FROM
  filtered m
INNER JOIN
  (
    SELECT
      client_id,
      MAX(submission_timestamp) AS latest_timestamp
    FROM
      filtered
    GROUP BY
      client_id
  ) lt
  ON m.client_id = lt.client_id
  AND m.submission_timestamp = lt.latest_timestamp
