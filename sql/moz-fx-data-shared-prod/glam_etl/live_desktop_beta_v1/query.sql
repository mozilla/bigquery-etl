-- Query for glam_etl.live_desktop_beta_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
MERGE INTO
  `moz-fx-data-glam-prod-fca7.glam_etl.live_desktop_beta_v1` T
USING
  (SELECT * FROM `moz-fx-data-shared-prod.telemetry_derived.glam_extract_firefox_beta_v1`) S
ON
  T.version = S.app_version
  AND T.os = S.os
  AND T.build_id = S.app_build_id
  AND T.process = S.process
  AND T.metric = S.metric
  AND T.metric_key = S.key
  AND T.client_agg_type = S.client_agg_type
  AND T.metric_type = S.metric_type
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (
      version,
      os,
      build_id,
      process,
      metric,
      metric_key,
      client_agg_type,
      metric_type,
      total_users,
      histogram,
      percentiles,
      total_sample,
      non_norm_histogram,
      non_norm_percentiles
    )
  VALUES
    (
      S.app_version,
      S.os,
      S.app_build_id,
      S.process,
      S.metric,
      S.key,
      S.client_agg_type,
      S.metric_type,
      S.total_users,
      S.histogram,
      S.percentiles,
      S.total_sample,
      S.non_norm_histogram,
      S.non_norm_percentiles
    )
  WHEN MATCHED
THEN
  UPDATE
    SET T.total_users = S.total_users,
    T.histogram = S.histogram,
    T.percentiles = S.percentiles,
    T.total_sample = S.total_sample,
    T.non_norm_histogram = S.non_norm_histogram,
    T.non_norm_percentiles = S.non_norm_percentiles
