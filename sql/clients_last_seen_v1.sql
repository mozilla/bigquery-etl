WITH current_sample AS (
  SELECT

    -- This last_date struct is used to record the last date that a particular
    -- client met various criteria; we record a null date if the client does
    -- not meet a given criterion.
    STRUCT (
      IF(
        country IN ('US', 'FR', 'DE', 'UK', 'CA'),
        submission_date_s3,
        NULL
      ) AS seen_in_tier1_country,
      IF(
        scalar_parent_browser_engagement_total_uri_count_sum >= 5
        submission_date_s3,
        NULL
      ) AS visited_5_or_more_uris
    ) AS last_date,
    submission_date_s3 AS last_seen_date,
    * EXCEPT (submission_date_s3)
  FROM
    clients_daily_v6
  WHERE
    submission_date_s3 = @submission_date
), previous AS (
  SELECT
    * EXCEPT (submission_date,
      generated_time)
  FROM
    analysis.clients_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND last_seen_date > DATE_SUB(@submission_date, INTERVAL 28 DAY)
)
SELECT
  @submission_date AS submission_date,
  CURRENT_DATETIME() AS generated_time,
  
  -- merge last_date values
  STRUCT (
    COALESCE(
      current_sample.last_date.seen_in_tier1_country,
      previous.last_date.seen_in_tier1_country
    ) AS seen_in_tier1_country,
    COALESCE(
      current_sample.last_date.visited_5_or_more_uris,
      previous.last_date.visited_5_or_more_uris
    ) AS visited_5_or_more_uris
  ) AS last_date,
  
  IF(current_sample.client_id IS NOT NULL,
    current_sample,
    previous).* EXCEPT (last_date)
FROM
  current_sample
FULL JOIN
  previous
USING
  (client_id)
