-- Query for org_mozilla_fenix_derived.recorded_targeting_context_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
WITH filtered AS (
    SELECT
        m.client_info.client_id as client_id,
        m.submission_timestamp,
        m.metrics.object.nimbus_system_recorded_nimbus_context as context
    FROM `moz-fx-data-shared-prod.org_mozilla_fenix.metrics` m
    WHERE 1=1
      AND m.metrics.object.nimbus_system_recorded_nimbus_context IS NOT NULL
      AND DATE(m.submission_timestamp) = @submission_date
    AND m.normalized_channel = 'release'
    AND m.app_version_major >= 135
    )
SELECT
    m.client_id,
    DATE(m.submission_timestamp) as submission_date,
    CAST(JSON_VALUE(context, '$.androidSdkVersion') as int) as androidSdkVersion,
    CAST(JSON_VALUE(context, '$.appVersion') as string) as androidSdkVersion,
    CAST(JSON_VALUE(context, '$.daysSinceInstall') as int) as daysSinceInstall,
    CAST(JSON_VALUE(context, '$.daysSinceUpdate') as int) as daysSinceUpdate,
    CAST(JSON_VALUE(context, '$.deviceManufacturer') as string) as deviceManufacturer,
    CAST(JSON_VALUE(context, '$.deviceModel') as string) as deviceModel,
    CAST(JSON_VALUE(context, '$.eventQueryValues.daysOpenedInLast28') as int) as eventQuery_daysOpenedInLast28,
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmCampaign') as string) as installReferrerResponseUtmCampaign,
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmContent') as string) as installReferrerResponseUtmContent,
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmMedium') as string) as installReferrerResponseUtmMedium,
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmSource') as string) as installReferrerResponseUtmSource,
    CAST(JSON_VALUE(context, '$.installReferrerResponseUtmTerm') as string) as installReferrerResponseUtmTerm,
    CAST(JSON_VALUE(context, '$.isFirstRun') as boolean) as isFirstRun,
    CAST(JSON_VALUE(context, '$.language') as string) as language,
    CAST(JSON_VALUE(context, '$.locale') as string) as locale,
    CAST(JSON_VALUE(context, '$.region') as string) as region,
    context
FROM filtered m
    INNER JOIN (
    SELECT client_id, MAX(submission_timestamp) as latest_timestamp
    FROM filtered
    GROUP BY client_id
    ) lt
ON m.client_id = lt.client_id AND m.submission_timestamp = lt.latest_timestamp
