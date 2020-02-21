/*

Daily install statistics to power AMO stats pages. See Bug 1572873.

This query looks backward in time by two days in order to allow
some delay in installs actually being reported, which means each
submission_date partition actually reflects installs from two days
prior. We adjust for this in the user-facing view on top of this
table (telemetry.amo_stats_installs), where we replace
`submission_date` with `install_date`.

*/
--
WITH base AS (
  SELECT
    submission_date,
    client_id,
    addon.addon_id,
    SAFE.DATE_FROM_UNIX_DATE(install_day) AS install_date,
    DATE_SUB(@submission_date, INTERVAL 2 DAY) AS target_install_date,
  FROM
    telemetry.clients_daily
  CROSS JOIN
    UNNEST(active_addons) AS addon
)
--
SELECT
  @submission_date AS submission_date,
  addon_id,
  COUNT(DISTINCT client_id) AS installs,
FROM
  base
WHERE
  install_date = target_install_date
  AND submission_date BETWEEN target_install_date AND @submission_date
GROUP BY
  addon_id
