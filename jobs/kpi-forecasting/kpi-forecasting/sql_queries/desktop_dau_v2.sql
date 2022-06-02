SELECT
     submission_date,
     SUM(dau) AS DAU
FROM telemetry.firefox_desktop_usage_2021
GROUP BY 1
ORDER BY 1
