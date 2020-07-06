SELECT
  * EXCEPT (histogram_aggregates),
  ARRAY_CONCAT(
    COALESCE(parent.histogram_aggregates, []),
    COALESCE(content.histogram_aggregates, [])
  ) AS histogram_aggregates,
FROM
  clients_daily_histogram_aggregates_parent_v1 AS parent
FULL JOIN
  clients_daily_histogram_aggregates_content_v1 AS content
USING
  (sample_id, client_id, submission_date, os, app_version, app_build_id, channel)
WHERE
  parent.submission_date = @submission_date
  AND content.submission_date = @submission_date
