{{ header }}
SELECT
    channel,
    MAX(major_version)
FROM
    `moz-fx-data-shared-prod.telemetry.releases_latest`
WHERE
    channel={{ app_id_channel }}
GROUP BY
    channel
