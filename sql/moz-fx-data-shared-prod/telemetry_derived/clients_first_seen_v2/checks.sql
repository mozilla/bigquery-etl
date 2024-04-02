#fail
{{ is_unique(columns=["client_id"]) }}

#fail
ASSERT(
  SELECT
    COUNTIF(first_seen.client_id IS NULL)
  FROM
    `{{ project_id }}.{{ dataset_id }}.clients_daily_v6` AS daily
  LEFT JOIN
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}` AS first_seen
    USING (client_id)
  WHERE
    submission_date = @submission_date
) = 0;

#fail
ASSERT(
  SELECT
    COUNTIF(first_seen.client_id IS NULL)
  FROM
    `{{ project_id }}.telemetry.new_profile` AS new_profile
  LEFT JOIN
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}` AS first_seen
    USING (client_id)
  WHERE
    DATE(submission_timestamp) = @submission_date
) = 0;

-- TODO: from https://mozilla-hub.atlassian.net/browse/DS-3102:
-- ratio of new profiles reporting NPP, FSP, MP as the first ping (we if this ratio diverges wildly,
-- we’d want to know) what's the baseline here?
