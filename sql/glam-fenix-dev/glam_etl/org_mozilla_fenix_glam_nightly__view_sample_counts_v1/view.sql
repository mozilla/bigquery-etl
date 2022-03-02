-- view for org_mozilla_fenix_glam_nightly__view_sample_counts_v1;
CREATE OR REPLACE VIEW
  `glam-fenix-dev.glam_etl.org_mozilla_fenix_glam_nightly__view_sample_counts_v1`
AS
WITH histogram_data AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    h1.metric,
    h1.key,
    h1.agg_type,
    h1.value
  FROM
    `glam-fenix-dev.glam_etl.org_mozilla_fenix_glam_nightly__clients_histogram_aggregates_v1`,
    UNNEST(histogram_aggregates) h1
),
all_clients AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    s1.metric,
    s1.key,
    s1.agg_type,
    s1.value
  FROM
    `glam-fenix-dev.glam_etl.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1`,
    UNNEST(scalar_aggregates) s1
  WHERE
    s1.agg_type IN ('count', 'false', 'true')
  UNION ALL
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    v1.key,
    agg_type,
    v1.value
  FROM
    histogram_data,
    UNNEST(value) v1
),
-- Cross join with the attribute combinations to reduce the query complexity
-- with respect to the number of operations. A table with n rows cross joined
-- with a combination of m attributes will generate a new table with n*m rows.
-- The glob ("*") symbol can be understood as selecting all of values belonging
-- to that group.
static_combos AS (
  SELECT
    combos.*
  FROM
    UNNEST(
      ARRAY<STRUCT<ping_type STRING, os STRING, app_build_id STRING>>[
        (NULL, NULL, NULL),
        (NULL, NULL, "*"),
        (NULL, "*", NULL),
        ("*", NULL, NULL),
        (NULL, "*", "*"),
        ("*", NULL, "*"),
        ("*", "*", NULL),
        ("*", "*", "*")
      ]
    ) AS combos
),
all_combos AS (
  SELECT
    table.* EXCEPT (ping_type, os, app_build_id),
    COALESCE(combo.ping_type, table.ping_type) AS ping_type,
    COALESCE(combo.os, table.os) AS os,
    COALESCE(combo.app_build_id, table.app_build_id) AS app_build_id
  FROM
    all_clients table
  CROSS JOIN
    static_combos combo
)
SELECT
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  key,
  metric,
  SUM(value) AS total_sample
FROM
  all_combos
GROUP BY
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  key,
  agg_type
