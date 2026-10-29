#fail
{{ min_row_count(1, "event_date = @submission_date") }}

#fail
{{ not_null(["event_date"], "event_date = @submission_date") }}

#fail
{{ is_unique(["event_date", "hostname", "landing_locale", "landing_page", "landing_page_type", "download_page_type", "country", "source", "medium", "campaign", "channel_group", "ad_platform", "product", "platform", "release_channel", "download_language", "method"], "event_date = @submission_date") }}

#warn
{{ row_count_within_past_partitions_avg(7, 50) }}
-- Warns when too many downloads have no landing page, which usually means the session join failed.

#warn
WITH coverage AS (
  SELECT
    SUM(download_count) AS total_downloads,
    SUM(IF(landing_page_type IS NULL, download_count, 0)) AS no_landing_downloads
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    event_date = @submission_date
)
SELECT
  IF(
    SAFE_DIVIDE(no_landing_downloads, total_downloads) > 0.02,
    ERROR(
      FORMAT(
        'More than 2 percent of product_download events on %t have no landing page (%d of %d)',
        @submission_date,
        no_landing_downloads,
        total_downloads
      )
    ),
    NULL
  )
FROM
  coverage;
