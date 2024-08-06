-- Query for mozilla_org_derived.blogs_goals_v2
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  *
FROM
  table
WHERE
  submission_date = @submission_date
