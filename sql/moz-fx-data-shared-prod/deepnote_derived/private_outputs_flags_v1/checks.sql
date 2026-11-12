#warn
ASSERT (
  SELECT
    COUNT(*)
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.private_outputs_flags_v1`
  WHERE
    is_private
    OR flag = 'schedule_access_revoked'
) = 0
AS
  "Private Deepnote projects have issues or schedules lost access, see deepnote_derived.private_outputs_flags_v1.";
