-- query for org_mozilla_fenix_glam_nightly__scalar_percentiles_v1;
WITH flat_clients_scalar_aggregates AS (
  SELECT
    * EXCEPT (scalar_aggregates)
  FROM
    glam_etl.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1
  CROSS JOIN
    UNNEST(scalar_aggregates)
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
    flat_clients_scalar_aggregates table
  CROSS JOIN
    static_combos combo
),
percentiles AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(DISTINCT(client_id)) AS total_users,
    APPROX_QUANTILES(value, 100) AS aggregates
  FROM
    all_combos
  GROUP BY
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    client_agg_type
)
SELECT
  * REPLACE (
    mozfun.glam.map_from_array_offsets([5.0, 25.0, 50.0, 75.0, 95.0], aggregates) AS aggregates
  )
FROM
  percentiles
