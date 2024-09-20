SELECT
  @submission_date AS submission_date,
  sd.country,
  sd.app_name,
  sd.adjust_network,
  sd.attribution_medium,
  sd.attribution_source,
  sd.first_seen_year,
  sd.channel,
  sd.install_source,
  sd.is_default_browser,
  sd.os_grouped,
  sd.segment,
  sd.dau,
  AVG(dau) OVER (
    PARTITION BY
      country,
      app_name,
      adjust_network,
      attribution_medium,
      attribution_source,
      first_seen_year,
      channel,
      install_source,
      is_default_browser,
      os_grouped,
      segment
    ORDER BY
      submission_date
    ROWS BETWEEN
      27 PRECEDING
      AND CURRENT ROW
  ) AS ma_28_dau
FROM
  `moz-fx-data-shared-prod.telemetry.segmented_dau` sd
WHERE
  submission_date
  BETWEEN DATE_SUB(@submission_date, INTERVAL 30 DAY)
  AND @submission_date
QUALIFY
  RANK() OVER (ORDER BY submission_date DESC) = 1
