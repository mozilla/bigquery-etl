CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_last_seen_v1`
AS
SELECT
  -- We cannot use UDFs in a view, so we paste the body of udf.bitpos(bits) literally here.
  CAST(SAFE.LOG(days_seen_bits & -days_seen_bits, 2) AS INT64) AS days_since_seen,
  CAST(
    SAFE.LOG(days_visited_5_uri_bits & -days_visited_5_uri_bits, 2) AS INT64
  ) AS days_since_visited_5_uri,
  CAST(
    SAFE.LOG(days_opened_dev_tools_bits & -days_opened_dev_tools_bits, 2) AS INT64
  ) AS days_since_opened_dev_tools,
  CAST(
    SAFE.LOG(days_created_profile_bits & -days_created_profile_bits, 2) AS INT64
  ) AS days_since_created_profile,
  CASE
    CAST(
      BIT_COUNT(
        days_visited_5_uri_bits & `moz-fx-data-shared-prod.udf.bitmask_range`(2, 6)
      ) >= 1 AS INT64
    ) + CAST(
      BIT_COUNT(
        days_visited_5_uri_bits & `moz-fx-data-shared-prod.udf.bitmask_range`(8, 7)
      ) >= 2 AS INT64
    ) + CAST(
      BIT_COUNT(
        days_visited_5_uri_bits & `moz-fx-data-shared-prod.udf.bitmask_range`(15, 7)
      ) >= 2 AS INT64
    ) + CAST(
      BIT_COUNT(
        days_visited_5_uri_bits & `moz-fx-data-shared-prod.udf.bitmask_range`(22, 7)
      ) >= 2 AS INT64
    )
  WHEN
    4
  THEN
    'regular_users_v1'
  WHEN
    0
  THEN
    'new_irregular_users_v1'
  ELSE
    'semi_regular_users_v1'
  END
  AS segment_usage_regularity_v1,
  * EXCEPT (
    active_experiment_id,
    scalar_parent_dom_contentprocess_troubled_due_to_memory_sum,
    total_hours_sum,
    histogram_parent_devtools_developertoolbar_opened_count_sum,
    active_experiment_branch
  ) REPLACE(
    IFNULL(country, '??') AS country,
    IFNULL(city, '??') AS city,
    IFNULL(geo_subdivision1, '??') AS geo_subdivision1,
    IFNULL(geo_subdivision2, '??') AS geo_subdivision2,
    ARRAY(
      SELECT AS STRUCT
        *,
        CAST(SAFE.LOG(bits & -bits, 2) AS INT64) AS days_since_seen
      FROM
        UNNEST(days_seen_in_experiment)
    ) AS days_seen_in_experiment
  ),
  -- TODO: Announce and remove this temporary field.
  CAST(sample_id AS STRING) AS _sample_id_string
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v1`
