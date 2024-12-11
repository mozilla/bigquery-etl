-- Script for glam_etl.glam_fenix_release_aggregates_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
MERGE INTO
  `moz-fx-glam-prod.glam_etl.glam_fenix_release_aggregates_v1` T
  USING (
    SELECT
      *
    FROM
      `moz-fx-data-glam-prod-fca7.glam_etl.org_mozilla_fenix_glam_release__extract_probe_counts_v1`
  ) S
  ON T.version = S.version
  AND T.ping_type = S.ping_type
  AND T.os = S.os
  AND T.build_id = S.build_id
  AND T.metric = S.metric
  AND T.metric_type = S.metric_type
  AND T.metric_key = S.metric_key
  AND T.client_agg_type = S.client_agg_type
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (
      version,
      ping_type,
      os,
      build_id,
      build_date,
      metric,
      metric_type,
      metric_key,
      client_agg_type,
      total_users,
      histogram,
      percentiles,
      non_norm_histogram,
      non_norm_percentiles,
      total_sample
    )
  VALUES
    (
      S.version,
      S.ping_type,
      S.os,
      S.build_id,
      S.build_date,
      S.metric,
      S.metric_type,
      S.metric_key,
      S.client_agg_type,
      S.total_users,
      S.histogram,
      NULL,
      S.non_norm_histogram,
      NULL,
      S.total_sample
    )
  WHEN MATCHED
THEN
  UPDATE
    SET T.total_users = S.total_users,
    T.histogram = S.histogram,
    T.percentiles = NULL,
    T.non_norm_histogram = S.non_norm_histogram,
    T.non_norm_percentiles = NULL,
    T.total_sample = S.total_sample
