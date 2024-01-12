WITH base AS (
  SELECT
    *,
    EXTRACT(YEAR FROM submission_date) AS submission_year,
  FROM
    telemetry_derived.firefox_desktop_exact_mau28_by_dimensions_v2
  WHERE
    -- We completely recreate this table every night since the source table
    -- is small and this query windows over a large time range.
    submission_date >= '2017-01-01'
),
--
-- We need a continuous list of dates to aid us later in transforming into
-- a dense representation where each slice appears every day.
continuous_dates AS (
  SELECT
    submission_date,
    EXTRACT(YEAR FROM submission_date) AS submission_year
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY(
        (SELECT MIN(submission_date) FROM base),
        (SELECT MAX(submission_date) FROM base)
      )
    ) AS submission_date
),
--
-- We aggregate all the counts for a given slice into an array per year,
-- so that we can operate on entire years of data consistently.
nested_counts_by_slice_year_sparse AS (
  SELECT
    submission_year,
    id_bucket,
    activity_segment,
    os,
    channel,
    `source`,
    medium,
    campaign,
    content,
    country,
    distribution_id,
    ARRAY_AGG(STRUCT(submission_date, dau, wau, mau, new_profiles)) AS counts_array,
  FROM
    base
  GROUP BY
    submission_year,
    id_bucket,
    activity_segment,
    os,
    channel,
    `source`,
    medium,
    campaign,
    content,
    country,
    distribution_id
),
--
-- Now we fill out the array of counts, injecting empty values for any days on which
-- a given slice does not appear.
nested_counts_by_slice_year_dense AS (
  SELECT
    * REPLACE (
      ARRAY(
        SELECT
          STRUCT(submission_date, c.dau, c.mau, c.wau, c.new_profiles)
        FROM
          UNNEST(counts_array) AS c
        FULL JOIN
          -- This is a "correlated subquery" that references a field from the
          -- outer query in order to filter to just the relevant year.
          (
            SELECT
              submission_date
            FROM
              continuous_dates
            WHERE
              submission_year = nested_counts_by_slice_year_sparse.submission_year
          )
          USING (submission_date)
      ) AS counts_array
    )
  FROM
    nested_counts_by_slice_year_sparse
),
--
-- We can now explode the array, leading to a flat representation which now includes
-- one row per day per slice, regardless of whether any clients were seen for that
-- slice on that particular day.
exploded AS (
  SELECT
    c.*,
    nested_counts_by_slice_year_dense.* EXCEPT (counts_array)
  FROM
    nested_counts_by_slice_year_dense,
    UNNEST(counts_array) AS c
)
--
-- Finally, we can do our windowed SUM to materialize CDOU.
SELECT
  SUM(dau) OVER (year_slice) AS cdou,
  SUM(new_profiles) OVER (year_slice) AS cumulative_new_profiles,
  (source IS NOT NULL OR campaign IS NOT NULL) AS attributed,
  exploded.*,
  cc.name AS country_name,
FROM
  exploded
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` AS cc
  ON (exploded.country = cc.code)
WINDOW
  year_slice AS (
    PARTITION BY
      submission_year,
      id_bucket,
      activity_segment,
      os,
      channel,
      `source`,
      medium,
      campaign,
      content,
      country,
      distribution_id
    ORDER BY
      submission_date
  )
