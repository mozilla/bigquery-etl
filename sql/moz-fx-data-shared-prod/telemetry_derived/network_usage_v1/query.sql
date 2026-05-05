WITH per_user AS (
  SELECT
    EXTRACT(DATE FROM m.submission_timestamp) AS submission_date,
    SUM(
      CASE
        WHEN m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb IS NOT NULL
          THEN COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y0_N1Sys"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y1_N1"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y2_N3Oth"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y3_N3BasicLead"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y4_N3BasicBg"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y5_N3BasicOth"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y6_N3ContentLead"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y7_N3ContentBg"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y8_N3ContentOth"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y9_N3FpLead"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y10_N3FpBg"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y11_N3FpOth"
              ),
              0
            )
      END
    ) / 1000000 AS non_private_usage_gb,
    SUM(
      CASE
        WHEN m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb IS NOT NULL
          THEN COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y12_P1Sys"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y13_P1"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y14_P3Oth"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y15_P3BasicLead"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y16_P3BasicBg"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y17_P3BasicOth"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y18_P3ContentLead"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y19_P3ContentBg"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y20_P3ContentOth"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y21_P3FpLead"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y22_P3FpBg"
              ),
              0
            ) + COALESCE(
              `moz-fx-data-shared-prod.udf.get_key`(
                m.payload.processes.parent.keyed_scalars.networking_data_transferred_v3_kb,
                "Y23_P3FpOth"
              ),
              0
            )
      END
    ) / 1000000 AS private_usage_gb,
  FROM
    `moz-fx-data-shared-prod.telemetry.main` m
  WHERE
    m.normalized_channel = 'release'
    AND DATE(m.submission_timestamp) = @submission_date
  GROUP BY
    m.client_id,
    submission_date
)
SELECT
  submission_date,
  SUM(non_private_usage_gb) AS total_non_private_gb,
  SUM(private_usage_gb) AS total_private_gb,
  AVG(private_usage_gb) AS mean_private_gb,
  APPROX_QUANTILES(private_usage_gb, 100 IGNORE NULLS)[OFFSET(5)] AS pct_5,
  APPROX_QUANTILES(private_usage_gb, 100 IGNORE NULLS)[OFFSET(25)] AS pct_25,
  APPROX_QUANTILES(private_usage_gb, 100 IGNORE NULLS)[OFFSET(50)] AS pct_50,
  APPROX_QUANTILES(private_usage_gb, 100 IGNORE NULLS)[OFFSET(75)] AS pct_75,
  APPROX_QUANTILES(private_usage_gb, 100 IGNORE NULLS)[OFFSET(95)] AS pct_95,
  APPROX_QUANTILES(private_usage_gb, 100 IGNORE NULLS)[OFFSET(99)] AS pct_99,
FROM
  per_user
GROUP BY
  submission_date
