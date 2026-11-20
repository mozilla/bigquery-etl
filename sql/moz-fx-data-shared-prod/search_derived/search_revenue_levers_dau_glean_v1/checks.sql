#fail
-- Rows come from the DAU aggregates, so a partition written before search_dau_aggregates_glean or
-- baseline_active_users is complete would be short or empty without any error. Require default-engine
-- DAU for every partner on both desktop and mobile.
SELECT
  IF(
    COUNT(DISTINCT IF(dau_w_engine_as_default > 0, CONCAT(partner, '/', device), NULL)) < 6,
    ERROR(
      FORMAT(
        "Only %d of 6 partner and device combinations have default-engine DAU for %t. Check that the DAU inputs are written for the day.",
        COUNT(DISTINCT IF(dau_w_engine_as_default > 0, CONCAT(partner, '/', device), NULL)),
        @submission_date
      )
    ),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;
