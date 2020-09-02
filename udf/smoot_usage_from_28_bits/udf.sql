/*

Calculates a variety of metrics based on bit patterns of daily usage
for the smoot_usage_* tables.

*/
CREATE OR REPLACE FUNCTION udf.smoot_usage_from_28_bits(
  bit_arrays ARRAY<STRUCT<days_created_profile_bits INT64, days_active_bits INT64>>
) AS (
  (
    WITH unnested AS (
      SELECT
        days_active_bits AS bits,
        udf.pos_of_trailing_set_bit(days_created_profile_bits) AS dnp,
        udf.pos_of_trailing_set_bit(days_active_bits) AS days_since_active,
        udf.bitcount_lowest_7(days_active_bits) AS active_days_in_week
      FROM
        UNNEST(bit_arrays)
    )
    SELECT AS STRUCT
      COUNTIF(bits > 0) = 0 AS is_empty_group,
      STRUCT(
        COUNTIF(days_since_active < 1) AS dau,
        COUNTIF(days_since_active < 7) AS wau,
        COUNTIF(days_since_active < 28) AS mau,
        SUM(active_days_in_week) AS active_days_in_week
      ) AS day_0,
      STRUCT(COUNTIF(dnp = 6) AS new_profiles) AS day_6,
      STRUCT(
        COUNTIF(dnp = 13) AS new_profiles,
        COUNTIF(udf.active_n_weeks_ago(bits, 1)) AS active_in_week_0,
        COUNTIF(udf.active_n_weeks_ago(bits, 0)) AS active_in_week_1,
        COUNTIF(
          udf.active_n_weeks_ago(bits, 1)
          AND udf.active_n_weeks_ago(bits, 0)
        ) AS active_in_weeks_0_and_1,
        COUNTIF(dnp = 13 AND udf.active_n_weeks_ago(bits, 1)) AS new_profile_active_in_week_0,
        COUNTIF(dnp = 13 AND udf.active_n_weeks_ago(bits, 0)) AS new_profile_active_in_week_1,
        COUNTIF(
          dnp = 13
          AND udf.active_n_weeks_ago(bits, 1)
          AND udf.active_n_weeks_ago(bits, 0)
        ) AS new_profile_active_in_weeks_0_and_1
      ) AS day_13
    FROM
      unnested
  )
);
