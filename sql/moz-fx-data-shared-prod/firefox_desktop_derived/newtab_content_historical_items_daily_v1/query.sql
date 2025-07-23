SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_content_items_daily_v1`
WHERE
  submission_date = @submission_date
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_items_daily_v1`
WHERE
  submission_date = @submission_date
