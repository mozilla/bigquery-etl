#fail
-- Rows come from the DAU aggregates, and dau comes from baseline_active_users and mobile_active_users.
-- A partition written before search_dau_aggregates_glean is complete would be short or empty, and
-- one written before the eligible-DAU sources are complete would have a NULL or zero dau, either
-- without any error. Require both default-engine DAU and dau to be positive for every partner on
-- both desktop and mobile.
WITH combinations AS (
  SELECT
    partner,
    device,
    SUM(dau_w_engine_as_default) AS default_engine_dau,
    SUM(dau) AS dau
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
  GROUP BY
    partner,
    device
)
SELECT
  IF(
    COUNTIF(default_engine_dau > 0 AND dau > 0) < 6,
    ERROR(
      FORMAT(
        "Only %d of 6 partner and device combinations have both default-engine DAU and dau for %t. Check that the DAU inputs, including baseline_active_users and mobile_active_users, are written for the day.",
        COUNTIF(default_engine_dau > 0 AND dau > 0),
        @submission_date
      )
    ),
    NULL
  )
FROM
  combinations;
