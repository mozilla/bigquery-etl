
#warn
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-marketing-prod.ga_derived.blogs_goals_v2`
  WHERE
    date = @submission_date
  GROUP BY
    date,
    visit_identifier
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['date', 'visit_identifier'] to be unique where date = @submission_date.)"
    ),
    NULL
  );

#warn
SELECT
  IF(
    ROUND(
      (COUNTIF(NOT REGEXP_CONTAINS(visit_identifier, r"^[0-9]+\.{1}[0-9]+\-{1}[0-9]+$"))) / COUNT(
        *
      ) * 100,
      2
    ) > 0,
    ERROR("Warn - some visit_identifier not matching expected pattern"),
    NULL
  )
FROM
  `moz-fx-data-marketing-prod.ga_derived.blogs_goals_v2`
WHERE
  date = @submission_date;
