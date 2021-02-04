SELECT
  submission_date,
  product,
  SPLIT(app_version, '.')[offset(0)] AS app_version,
  os,
  normalized_channel,
  country,
  COUNTIF(udf.pos_of_trailing_set_bit(days_created_profile_bits) = 6) AS new_profiles,
  COUNTIF(
    udf.pos_of_trailing_set_bit(days_created_profile_bits) = 6
    AND BIT_COUNT(days_seen_bits << 1 & udf.bitmask_lowest_7()) > 0
  ) AS day_2_7_activated,
FROM
  `moz-fx-data-shared-prod.telemetry.nondesktop_clients_last_seen`
WHERE
  submission_date = @submission_date
  AND contributes_to_2020_kpi
GROUP BY
  submission_date,
  product,
  app_version,
  os,
  normalized_channel,
  country
