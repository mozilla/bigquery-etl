-- view for org_mozilla_fenix_glam_nightly__view_user_counts_v1;
CREATE OR REPLACE VIEW
  `glam-fenix-dev.glam_etl.org_mozilla_fenix_glam_nightly__view_user_counts_v1`
AS
WITH all_clients AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel
  FROM
    `glam-fenix-dev`.glam_etl.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1
  UNION ALL
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel
  FROM
    `glam-fenix-dev`.glam_etl.org_mozilla_fenix_glam_nightly__clients_histogram_aggregates_v1
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
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_combos
GROUP BY
  ping_type,
  os,
  app_version,
  app_build_id,
  channel
