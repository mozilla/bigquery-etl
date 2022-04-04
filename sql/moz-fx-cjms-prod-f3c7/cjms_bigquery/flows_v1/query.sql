SELECT
  DATE(`timestamp`) AS submission_date,
  flow_id,
  MIN(`timestamp`) AS flow_started,
  ARRAY_AGG(
    IF(
      user_id IS NULL,
      NULL,
      STRUCT(user_id AS fxa_uid, `timestamp` AS fxa_uid_timestamp)
    ) IGNORE NULLS
    ORDER BY
      `timestamp` DESC
    LIMIT
      1
  )[SAFE_OFFSET(0)].*,
FROM
  mozdata.firefox_accounts.fxa_content_auth_stdout_events
WHERE
  DATE(`timestamp`) = @submission_date
  AND flow_id IS NOT NULL
GROUP BY
  submission_date,
  flow_id
