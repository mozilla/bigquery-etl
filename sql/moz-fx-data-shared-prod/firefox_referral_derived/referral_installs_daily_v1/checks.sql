-- Steve (Website) confirmed 2026-07-24 that the invitation token is 17 chars
-- long, not counting the stripped `fxrefer:` prefix. Warn (not fail) on drift so
-- we notice a Firefox-side format change early without blocking the pipeline —
-- test codes (e.g. TESTCODE01) and any pre-rollout data may not be 17 chars yet.

#warn
ASSERT (
  SELECT
    COUNTIF(LENGTH(invite_code) != 17)
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
) = 0
AS
  'Unexpected invite_code length: expected 17 chars after stripping the fxrefer: prefix.';
