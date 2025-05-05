-- Query for org_mozilla_ios_firefox_derived.nimbus_recorded_targeting_context_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
WITH filtered AS (
  SELECT
    m.client_info.client_id AS client_id,
    m.submission_timestamp,
    m.metrics.object.nimbus_system_recorded_nimbus_context AS context
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox.metrics` m
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
  CAST(JSON_VALUE(context, '$.appVersion') AS string) AS androidSdkVersion,
  CAST(JSON_VALUE(context, '$.daysSinceInstall') AS int) AS daysSinceInstall,
  CAST(JSON_VALUE(context, '$.daysSinceUpdate') AS int) AS daysSinceUpdate,
  CAST(
    JSON_VALUE(context, '$.eventQueryValues.daysOpenedInLast28') AS int
  ) AS eventQuery_daysOpenedInLast28,
  CAST(JSON_VALUE(context, '$.isDefaultBrowser') AS boolean) AS isDefaultBrowser,
  CAST(JSON_VALUE(context, '$.isFirstRun') AS boolean) AS isFirstRun,
  CAST(JSON_VALUE(context, '$.isPhone') AS boolean) AS isPhone,
  CAST(JSON_VALUE(context, '$.isReviewCheckerEnabled') AS boolean) AS isReviewCheckerEnabled,
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
