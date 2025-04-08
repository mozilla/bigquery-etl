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
  CAST(JSON_VALUE(context, '$.androidSdkVersion') AS int) AS androidSdkVersion,
  CAST(JSON_VALUE(context, '$.appVersion') AS string) AS androidSdkVersion,
  CAST(JSON_VALUE(context, '$.daysSinceInstall') AS int) AS daysSinceInstall,
  CAST(JSON_VALUE(context, '$.daysSinceUpdate') AS int) AS daysSinceUpdate,
  CAST(JSON_VALUE(context, '$.deviceManufacturer') AS string) AS deviceManufacturer,
  CAST(JSON_VALUE(context, '$.deviceModel') AS string) AS deviceModel,
  CAST(
    JSON_VALUE(context, '$.eventQueryValues.daysOpenedInLast28') AS int
  ) AS eventQuery_daysOpenedInLast28,
  CAST(
    JSON_VALUE(context, '$.installReferrerResponseUtmCampaign') AS string
  ) AS installReferrerResponseUtmCampaign,
  CAST(
    JSON_VALUE(context, '$.installReferrerResponseUtmContent') AS string
  ) AS installReferrerResponseUtmContent,
  CAST(
    JSON_VALUE(context, '$.installReferrerResponseUtmMedium') AS string
  ) AS installReferrerResponseUtmMedium,
  CAST(
    JSON_VALUE(context, '$.installReferrerResponseUtmSource') AS string
  ) AS installReferrerResponseUtmSource,
  CAST(
    JSON_VALUE(context, '$.installReferrerResponseUtmTerm') AS string
  ) AS installReferrerResponseUtmTerm,
  CAST(JSON_VALUE(context, '$.isFirstRun') AS boolean) AS isFirstRun,
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
