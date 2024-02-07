#fail
{{ is_unique(columns=["client_id"]) }}

#fail
ASSERT NOT EXISTS(
  SELECT
    daily.client_id
  FROM
    telemetry_derived.clients_daily_v6 daily
  LEFT JOIN
    telemetry_derived.clients_first_seen_v2 first_seen
    USING (client_id)
  WHERE
    submission_date = @submission_date
    AND first_seen.client_id IS NULL
);

#fail
ASSERT NOT EXISTS(
  SELECT
    new_profile.client_id
  FROM
    telemetry.new_profile
  LEFT JOIN
    telemetry_derived.clients_first_seen_v2 first_seen
    USING (client_id)
  WHERE
    DATE(submission_timestamp) = '2023-01-01'
    AND first_seen.client_id IS NULL
);

-- TODO: from https://mozilla-hub.atlassian.net/browse/DS-3102:
-- ratio of new profiles reporting NPP, FSP, MP as the first ping (we if this ratio diverges wildly,
-- we’d want to know) what's the baseline here?
